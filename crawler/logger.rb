require "logger"
require "io/console"
require "concurrent-ruby"
require "concurrent"
#require "jruby/synchronized"


# Multithreaded logging handler
class CrawlerLogger


  def initialize
    @logger_meta = Concurrent::Hash.new
  end

  def clear
    "\e[2J"
  end
  
  def log meta, crawl_queue

    @logger_meta[Thread.current.object_id] = meta
    log_msg = StringIO.new
    log_msg << clear
    log_msg << "LOG: \n"
    log_msg << "queue_size: #{crawl_queue.size} \n"
    @logger_meta.each do |thread_id, thread_meta|
      log_msg << "Thread #{thread_id} => #{thread_meta} \n"
    end

    puts log_msg.string
  end
end
