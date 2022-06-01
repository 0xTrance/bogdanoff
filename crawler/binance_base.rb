require "nokogiri"

class BinanceCrawler
  include Process
  include Mutex

  attr_accessor :starting_root_url

  BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix="

  # Initialize with root url to start the crawling process from
  # @param starting_root_url the url to start crawling from
  # @param crawler_count the number of crawlers to thread out
  def initialize starting_root_url, crawler_count
    @starting_root_url = starting_root_url
    @crawl_count = crawler_count
    @crawl_queue = [@starting_root_url]
    @crawl_queue_mutex = Mutex.new
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
    IO.popen curl &:read
  end

  # Extract child 
  # @param doc the node to parse
  # @param selector css selector
  def extract_children doc, selector
    return nil unless ["Prefix", "Key"].any?{|kw| selector.eql? kw}
    selector_target = doc.css selector
    selector_target[1..-1].map{|selector_node| selector_node.text}
  end

  def crawl
    url = nil
    @crawl_queue_mutex.synchronize do 
      url = @crawl_queue.pop
    end

    return if url.nil?

    doc = Nokogori::HTML.parse fetch url

    child_prefixes = extract_children doc, "Prefix"

    child_keys = extract_children doc, "Key"

    new_prefix_urls = child_prefixes.map do |prefix|
      BASE_URL + prefix
    end

    @crawl_queue_mutex.synchronize do
      @crawl_queue += new_prefix_urls
    end


    child_prefixes

  end
end
