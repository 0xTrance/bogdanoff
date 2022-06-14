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
require 'concurrent'
require "timeout"
require "io/console"
require_relative "./logger.rb"

Thread.abort_on_exception=true

class ChecksumValidationErr < StandardError;end

class BinanceCrawler
  include Process

  attr_reader :starting_root_url, :bucket_client,:bucket, :crawl_count
  attr_accessor :crawl_queue

  PREFIX_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix="
  KEY_BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/"

  # Initialize with root url to start the crawling process from
  # TODO: Handle further customization
  # @param starting_root_url the url to start crawling from
  # @param crawler_count the number of crawlers to thread out
  def initialize starting_root_url, crawler_count, thread_fetch_timeout, bucket_name

    @starting_root_url = starting_root_url
    @crawl_count = crawler_count

    @threads = Concurrent::Array.new
    @crawl_queue = Queue.new
    @crawl_queue.push @starting_root_url
    @thread_fetch_timeout = thread_fetch_timeout

    @bucket_client = Google::Cloud::Storage.new project_id: "arbtools-crawlers", credentials: "./arbtools-crawlers-crawler"
    @bucket = @bucket_client.bucket bucket_name
    puts "Authenticated to Bucket: #{@bucket_client}"
    @logger = CrawlerLogger.new
  end

  def up
    @threads = @crawl_count.times.map do |i|
      Thread.new {crawl}
    end
    @threads.map(&:join)
    @crawl_queue.close
  end

  # Make a request to binance with a particular url with relevant headers
  # @param url url to make a request to
  # @return response as plain text
  def fetch url
    curl=<<~CURL
      curl -Ss '#{url}' \
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

    upload = @bucket.create_file stream, bucket_path_with_extension
    puts "Uploaded: #{bucket_path_with_extension}"
  end

  def download file_raw_url, checksum_verify = false
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

      response_zip_stream = Zip::InputStream.new file_raw
      extract_target = response_zip_stream.get_next_entry
      extract_io = StringIO.new extract_target.get_input_stream.read
      upload_to_bucket extract_io, prefix
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

  def finished?
    all_idle_threads? and ! @crawl_queue.closed? and !actions?
  end

  def all_idle_threads?
    #pp @threads
    not @threads.empty? and @threads.all? do |thread|
      thread.status.eql? "sleep"
    end
  end

  def actions?
    not @crawl_queue.empty?
  end

  # Pop off a new task if it exists, else, make the thread sleep
  def pop_task wait = true
    begin
      Timeout.timeout  @thread_fetch_timeout do
        @crawl_queue.pop !wait
      end
    rescue Timeout::Error
      return nil
    end
  end

  # Allocate threads to crawl individual urls
  def crawl
    while not finished?
      url = pop_task

      @logger.log "status: \"#{Thread.current.status}\" crawling: \"#{url}\"", @crawl_queue

      next if url.nil?

      if url.end_with? ".zip"
        crawl_key url
      else
        crawl_prefix url
      end
    end
    
    pp "FINISHED EXECUTION for thread: #{Thread.current.object_id}"
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

    new_key_urls = child_keys.map do |key|
      if key.end_with? "CHECKSUM"
        nil
      else
        KEY_BASE_URL + key
      end
    end.compact

    new_jobs = new_key_urls + new_prefix_urls
    new_jobs.each do |url_job|
      @crawl_queue.push url_job
    end
  
  end

  # Crawl a key node
  # @param url of key to crawl
  def crawl_key url
    download url, true
  end
end


starting_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/"
crawler = BinanceCrawler.new starting_url, 100, 10, "bogdanoff"

pp "FINISHED INITALIZATION"

crawler.up
