require "nokogiri"
require "digest"
require "concurrent-ruby"
require "open-uri"
require "zip"
require "google/cloud/storage"
require "uri"
require "tempfile"
require "concurrent"
require "securerandom"

target_url = "https://data.binance.vision/data/spot/daily/aggTrades/AAVEBTC/AAVEBTC-aggTrades-2022-06-13.zip"

bucket_client = Google::Cloud::Storage.new project_id: "arbtools-crawlers", credentials: "./arbtools-crawlers-crawler"
bucket = bucket_client.bucket "bogdanoff"

class ChecksumValidationErr < StandardError;end

def upload_to_bucket stream, prefix, bucket
  bucket_path = prefix.delete_suffix ".zip"
  bucket_path_with_extension = bucket_path + ".csv"

  upload = bucket.create_file stream, bucket_path_with_extension
end

def download file_raw_url, bucket, checksum_verify = false
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
    upload_to_bucket extract_io, prefix, bucket
  rescue ChecksumValidationErr
    retry
  rescue => e
    raise e
  end
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end


def download_curl_full file_raw_url, bucket, checksum_verify = false

  checksum_url = file_raw_url + ".CHECKSUM"
  file_uri = URI::parse file_raw_url
  prefix = file_uri.path.split("/")[2..-1].join("/")
  file_name = SecureRandom.uuid

  begin 
    file_curl = <<~CURL
      curl -Ss -o ./#{file_name} '#{file_raw_url}'
    CURL
    IO.popen(file_curl, &:read)

    if checksum_verify
      checksum_curl = <<~CURL
        curl -Ss '#{checksum_url}'
      CURL
      checksum_response = IO.popen(checksum_curl, &:read)
      remote_checksum = checksum_response.split.first
      local_checksum = IO.popen("sha256sum ./#{file_name}", &:read).split.first
      raise ChecksumValidationErr unless remote_checksum.eql? local_checksum
    end
    ## upload
    bucket_path = prefix.delete_suffix ".zip"
    bucket_path_with_extension = bucket_path + ".csv"

    IO.popen("unzip -p ./#{file_name} | gsutil cp - gs://bogdanoff/#{bucket_path_with_extension} 2> /dev/null")
  ensure
    IO.popen("rm -rf ./#{file_name}", &:read)
  end
end

def download_curl file_raw_url, bucket, checksum_verify = false

  checksum_url = file_raw_url + ".CHECKSUM"
  file_uri = URI::parse file_raw_url
  prefix = file_uri.path.split("/")[2..-1].join("/")
  file_name = SecureRandom.uuid

  begin 
    file_curl = <<~CURL
      curl -Ss -o ./#{file_name} '#{file_raw_url}'
    CURL
    IO.popen(file_curl, &:read)

    if checksum_verify
      checksum_curl = <<~CURL
        curl -Ss '#{checksum_url}'
      CURL
      checksum_response = IO.popen(checksum_curl, &:read)
      remote_checksum = checksum_response.split.first
      local_checksum = IO.popen("sha256sum ./#{file_name}", &:read).split.first
      raise ChecksumValidationErr unless remote_checksum.eql? local_checksum
    end
    
    ## upload
    extract_io = StringIO.new IO.popen("unzip -p ./#{file_name}", &:read)
    upload_to_bucket extract_io, prefix, bucket

  ensure
    IO.popen("rm -rf ./#{file_name}", &:read)
  end
end

# ====== FULL RUBY

start_time = now
10.times.map do
  Thread.new do
    5.times.map do
      download target_url, bucket, true
    end
  end
end.each(&:join)

finish_time = now
puts "Parallel time download: #{finish_time - start_time}"


start_time = now
10.times.map do
  5.times do
    download target_url, bucket, true
  end
end
finish_time = now
puts "Sequential time download: #{finish_time - start_time}"

# ===== DOWNLOAD CURL

start_time = now
10.times.map do
  Thread.new do
    5.times.map do
      download_curl target_url, bucket, true
    end
  end
end.each(&:join)

finish_time = now
puts "Parallel time download_curl: #{finish_time - start_time}"


start_time = now
10.times.map do
  5.times do
    download_curl target_url, bucket, true
  end
end

finish_time = now
puts "Sequential time download_curl: #{finish_time - start_time}"


# ====== DOWNLOAD CURL FULL

start_time = now
10.times.map do
  Thread.new do
    5.times.map do
      download_curl_full target_url, bucket, true
    end
  end
end.each(&:join)

finish_time = now
puts "Parallel time download_curl_full: #{finish_time - start_time}"


start_time = now
10.times.map do
  5.times do
    download_curl_full target_url, bucket, true
  end
end

finish_time = now
puts "Sequential time download_curl_full: #{finish_time - start_time}"



