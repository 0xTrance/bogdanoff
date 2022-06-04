require "nokogiri"
require "digest"
require 'concurrent-ruby'
require "open-uri"
require "zip"
require "google/cloud/storage"
require "json"
require "logger"
require "uri"
require 'tempfile'

Thread.abort_on_exception=true

class ChecksumValidationErr < StandardError;end

class BinanceCrawler
  include Process

  attr_reader :starting_root_url, :bucket_client,:bucket, :crawl_count

  PREFIX_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix="
  KEY_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/"


  # Initialize with root url to start the crawling process from
  # @param starting_root_url the url to start crawling from
  # @param crawler_count the number of crawlers to thread out
  def initialize starting_root_url, crawler_count, bucket_name
    @starting_root_url = starting_root_url
    @crawl_count = crawler_count
    @thread_table = {}
    @thread_table_mutex = Mutex.new

    @crawl_queue = [@starting_root_url]
    @crawl_queue_mutex = Mutex.new
    @bucket_client = Google::Cloud::Storage.new project_id: "arbtools-crawlers", credentials: "./arbtools-crawlers-crawler"
    @bucket = @bucket_client.bucket bucket_name
    @logger = Logger.new $stderr
    @logger.level = Logger::WARN
    Google::Apis.logger = @logger

    puts "Authenticated to Bucket: #{@bucket_client}"
  end

  # Run crawler with the specified number of threads
  def up
    @crawl_count.times.map do |i|
      Thread.new {crawl}
    end.each(&:join)
  end

  # Determines if the thread should terminate
  # Will terminate if there are no more jobs left AND all the threads have completed its execution
  def terminate?
    terminate = false
    @thread_table_mutex.synchronize do
      @crawl_queue_mutex.synchronize do
        terminate = @thread_table.values.all?{|running| not running} and @crawl_queue.empty?
      end
    end
    terminate
  end

  # Make a request to binance with a particular url with relevant headers
  # @param url url to make a request to
  # @return response as plain text
  def fetch url
    curl=<<~CURL
      curl '#{url}' \
      -H 'authority: data.binance.vision' \
      -H 'sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"' \
      -H 'sec-ch-ua-mobile: ?0' \
      -H 'upgrade-insecure-requests: 1' \
      -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36' \
      -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
      -H 'sec-fetch-site: same-origin' \
      -H 'sec-fetch-mode: navigate' \
      -H 'sec-fetch-user: ?1' \
      -H 'sec-fetch-dest: document' \
      -H 'referer: https://data.binance.vision/?prefix=data/spot/daily/' \
      -H 'accept-language: en-GB,en-US;q=0.9,en;q=0.8' \
      --compressed
    CURL
    IO.popen curl, &:read
  end

  # Make a request to download a file by its url
  # @param url: to download
  def upload_to_bucket stream, prefix

    bucket_path = prefix.delete_suffix ".zip"
    bucket_path_with_extension = bucket_path + ".csv"

    @bucket.create_file stream, bucket_path_with_extension
  end

  def download file_raw_url, checksum_verify = false
    pp "Thread: #{Thread.current.object_id} \n #{@thread_table} \n #{@crawl_queue.count}"
    begin

      checksum_url = file_raw_url + ".CHECKSUM"
      file_uri = URI::parse file_raw_url
      prefix = file_uri.path.split("/")[2..-1].join("/")

      file_raw = URI.open file_uri
      if checksum_verify
        checksum_response = URI.open checksum_url
        remote_checksum = checksum_response.read.split.first
        local_checksum = Digest::SHA2.hexdigest file_raw.read
        raise ChecksumValidationErr unless remote_checksum.eql? local_checksum
      end


      Zip::File.open file_raw do |zipfile|
        entry = zipfile.glob('*.csv').first
        
        #zip_extracted_io = entry.get_input_stream.read
        extracted = StringIO.new entry.get_input_stream.read
        upload_to_bucket extracted, prefix 
      end
    rescue ChecksumValidationErr
      retry
    rescue => e
      raise e
    end
  end

  # Extract child 
  # @param doc the node to parse
  # @param selector css selector
  def extract_children doc, selector
    return [] unless ["prefix", "key"].any?{|kw| selector.eql? kw}
    selector_target = doc.css selector
    selector_target.map{|selector_node| selector_node.text}
  end


  def crawl_pool_up
    @thread_table_mutex.synchronize do
      thread_id = Thread.current.object_id
      @thread_table[thread_id] = true
    end
  end

  def crawl_pool_down
    @thread_table_mutex.synchronize do
      thread_id = Thread.current.object_id
      @thread_table[thread_id] = false
    end
  end

  # Allocate threads to crawl individual urls
  def crawl
    loop do
      crawl_pool_up
      url = nil
      @crawl_queue_mutex.synchronize do 
        url = @crawl_queue.pop
      end
      pp url

      if url.nil?
        crawl_pool_down

        if terminate?
          return
        else
          next
        end
      end

      if url.end_with? ".zip" 
        crawl_key url
      else
        crawl_prefix url
      end
      crawl_pool_down
    end
  end
  
  # Crawl a prefix node
  # @param url of prefix to crawl
  def crawl_prefix url
    doc = Nokogiri::HTML.parse fetch url
    child_prefixes = extract_children doc, "prefix"
    child_keys = extract_children doc, "key"

    new_prefix_urls = child_prefixes.map do |prefix|
      PREFIX_BASE_URL + prefix
    end
    @crawl_queue_mutex.synchronize do
      @crawl_queue += new_prefix_urls
    end

    new_key_urls = child_keys.map do |key|
      if key.end_with? "CHECKSUM"
        nil
      else
        KEY_BASE_URL + key
      end
    end.compact

    @crawl_queue_mutex.synchronize do
      @crawl_queue += new_key_urls
    end
  end

  # Crawl a key node
  # @param url of key to crawl
  def crawl_key url
    download url, true
  end
end


starting_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/"
crawler = BinanceCrawler.new starting_url, 1, "bogdanoff"

crawler.up
