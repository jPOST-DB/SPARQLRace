# sparqlrace.rb
require 'sinatra'
require 'net/http'
require 'json'
require 'logger'
require 'uri'
require 'concurrent'

# ============================================================
# 設定
# ============================================================
ENDPOINTS = {
  qlever:   ENV.fetch('QLEVER'),
  virtuoso: ENV.fetch('VIRTUOSO')
}.freeze

TIMEOUT_SEC = ENV.fetch('TIMEOUT_SEC', '5').to_f

HOP_BY_HOP = %w[
  connection keep-alive transfer-encoding te trailers
  upgrade proxy-authorization proxy-authenticate
].freeze

# ============================================================
# ロガー設定
# ============================================================
RACE_LOGGER = Logger.new($stdout).tap do |l|
  l.formatter = proc do |severity, time, _, msg|
    "#{time.iso8601(3)} [#{severity}] #{msg}\n"
  end
end

# ============================================================
# レーシングプロキシ本体
# ============================================================
class RacingProxy
  Result = Struct.new(:name, :status, :headers, :body, :elapsed_sec, :error)

  def initialize(endpoints, timeout: TIMEOUT_SEC, logger: RACE_LOGGER)
    @endpoints = endpoints
    @timeout   = timeout
    @log       = logger
  end

  def race(method:, path:, body: nil, req_headers: {}, request_id: nil)
    @log.info("[#{request_id}] START race method=#{method.upcase} path=#{path}")
    @log.debug("[#{request_id}] forward_headers=#{req_headers.inspect}")

    futures = @endpoints.map do |name, base_url|
      Concurrent::Future.execute do
        call_endpoint(name, base_url, method, path, body, req_headers, request_id)
      end
    end

    winner = wait_for_winner(futures, request_id)
    log_race_result(futures, request_id)

    winner
  end

  private

  def call_endpoint(name, base_url, method, path, body, req_headers, request_id)
    # エンドポイントBは /sparql を付ける
    target_path = (name == :qlever) ? "/sparql#{path == '/' ? '' : path}" : path
    uri = URI.parse("#{base_url}#{target_path}")

    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl      = (uri.scheme == 'https')
    http.open_timeout = @timeout
    http.read_timeout = @timeout

    req = build_request(method, uri, body, req_headers, request_id)
    res = http.request(req)

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    @log.info("[#{request_id}] endpoint=#{name} url=#{uri} status=#{res.code} elapsed=#{format('%.3f', elapsed)}s")

    Result.new(name, res.code.to_i, res.to_hash, res.body, elapsed, nil)
  rescue => e
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start rescue 0
    @log.warn("[#{request_id}] endpoint=#{name} ERROR #{e.class}: #{e.message} elapsed=#{format('%.3f', elapsed)}s")
    Result.new(name, nil, {}, nil, elapsed, e)
  end

  def build_request(method, uri, body, req_headers, request_id)
    klass = method.to_sym == :post ? Net::HTTP::Post : Net::HTTP::Get
    req   = klass.new(uri.request_uri)

    req_headers.each { |k, v| req[k] = v }
    req['X-Request-Id'] = request_id if request_id
    req['Host'] = "#{uri.host}:#{uri.port}"  # バックエンドのHostに上書き

    req.body = body if body && method.to_sym == :post
    req
  end

  def wait_for_winner(futures, request_id)
    deadline = Time.now + @timeout

    loop do
      futures.each do |f|
        if f.complete?
          result = f.value
          return result if result && result.error.nil?
        end
      end

      if futures.all?(&:complete?)
        return futures.map(&:value).min_by { |r| r&.elapsed_sec || Float::INFINITY }
      end

      raise 'Race timeout' if Time.now > deadline

      sleep 0.005
    end
  end

  def log_race_result(futures, request_id)
    futures.each do |f|
      next unless f.complete?
      r = f.value
      next unless r
      status = r.error ? "ERROR(#{r.error.class})" : "HTTP #{r.status}"
      @log.info("[#{request_id}] RESULT endpoint=#{r.name} #{status} elapsed=#{format('%.3f', r.elapsed_sec)}s")
    end
  end
end

# ============================================================
# Sinatra設定
# ============================================================
PROXY = RacingProxy.new(ENDPOINTS)

configure do
  set :bind, ENV.fetch('BIND', '0.0.0.0')
  set :port, ENV.fetch('PORT', '4567').to_i
end

# ============================================================
# ヘッダー抽出（RackのCONTENT_TYPE問題を考慮）
# ============================================================
def extract_forward_headers(env)
  {}.tap do |h|
    env
      .select { |k, _| k.start_with?('HTTP_') }
      .reject { |k, _| %w[HTTP_HOST HTTP_CONNECTION].include?(k) }
      .each do |k, v|
        header_name = k.sub(/^HTTP_/, '').split('_').map(&:capitalize).join('-')
        h[header_name] = v
      end

    h['Content-Type']   = env['CONTENT_TYPE']   if env['CONTENT_TYPE'] && !env['CONTENT_TYPE'].empty?
    h['Content-Length'] = env['CONTENT_LENGTH']  if env['CONTENT_LENGTH'] && !env['CONTENT_LENGTH'].empty?
  end
end

# ============================================================
# リクエスト処理
# ============================================================
def extract_sparql_query(req, method)
  case method
  when :get
    req.params['query']
  when :post
    content_type = req.env['CONTENT_TYPE'].to_s

    if content_type.include?('application/sparql-query')
      req.body.read  # ボディがそのままSPARQLクエリ
    elsif content_type.include?('application/json')
      body = req.body.read
      return nil if body.nil? || body.empty?
      JSON.parse(body)['query']
    elsif content_type.include?('application/x-www-form-urlencoded')
      req.params['query']
    else
      req.params['query']
    end
  end
rescue JSON::ParserError => e
  RACE_LOGGER.warn("JSON parse error: #{e.message}")
  nil
end

def url_encode_query(query)
  URI.encode_www_form_component(query)
end

def handle_race(env)
  req        = Rack::Request.new(env)
  method     = req.request_method.downcase.to_sym
  path       = req.fullpath
  request_id = req.env['HTTP_X_REQUEST_ID'] || SecureRandom.hex(8)

  sparql_query = extract_sparql_query(req, method)

  if sparql_query.nil? || sparql_query.empty?
    status 400
    content_type :json
    return JSON.generate({ error: 'Missing query parameter' })
  end

  RACE_LOGGER.info("[#{request_id}] SPARQL query: #{url_encode_query(sparql_query)}")

  # バックエンドには常にPOST + application/sparql-query で送る
  forward_headers = {
    'Content-Type' => 'application/sparql-query',
    'Accept'       => req.env['HTTP_ACCEPT'] || 'application/json'
  }

  result = PROXY.race(
    method:      :post,           # バックエンドは常にPOST
    path:        '/',
    body:        sparql_query,    # クエリ本文をそのまま送る
    req_headers: forward_headers,
    request_id:  request_id
  )

  if result.nil? || result.error
    status 502
    content_type :json
    return JSON.generate({ error: 'Both endpoints failed', detail: result&.error&.message })
  end

  result.headers.each do |k, v|
    next if HOP_BY_HOP.include?(k.downcase)
    response.headers[k] = Array(v).join(', ')
  end

  response.headers['X-Winner-Endpoint'] = result.name.to_s
  response.headers['X-Race-Elapsed']    = format('%.3f', result.elapsed_sec)
  response.headers['X-Request-Id']      = request_id

  status result.status
  result.body
end

get  '/' do handle_race(env) end
post '/' do handle_race(env) end
