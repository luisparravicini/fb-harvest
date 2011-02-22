require 'mechanize'

class DownloaderWorker
  def get(url, &b)
    agent = Mechanize.new
    agent.user_agent_alias = 'Windows IE 7'

    nodes, leaves = parse(download(agent, url))
    b.call(nodes, leaves, url)
  end

  protected

  def download(agent, url)
    retries = 0
    page = nil
    begin
      agent.get(url)
    rescue Errno::ECONNREFUSED, Timeout::Error, Net::HTTPInternalServerError,
    Errno::ETIMEDOUT, EOFError, SocketError, Mechanize::ResponseCodeError
      retries += 1
      puts " Error: #{$!.message}"
      if retries > 5
        puts "Too many retries"
        exit
      end
      sleep 15*retries
      retry
    end
  end

  def parse(page)
    links = page.links.select { |link| link.href =~ %r{/people/.} }
    links.map! { |link| { :url => link.href, :title => link.text } }

    links.partition { |link| link[:url] =~ %r{/directory/} }
  end
end

class Downloader
  def initialize
    @mutex = Mutex.new
    @workers = Hash.new
    @max_workers = 3
    @results = Hash.new
    @scheduler = Thread.new(@workers, @mutex, @stop_scheduler) do |workers, mutex|
      quit = false
      while not quit
        mutex.synchronize do
          workers.delete_if do |url, value|
            t, b, id = value
            alive = t.alive?

            b.call(t['nodes'], t['leaves'], url, id) if not alive

            not alive
          end
          quit = workers.empty? && Thread.current['stop']
        end
        sleep(0.5)
      end
    end
  end

  def get(url, id, &b)
    added = false
    while not added do
      @mutex.synchronize do
        if @workers.size <= @max_workers
          t = Thread.new(url, id) do |url, id|
            DownloaderWorker.new.get(url) do |nodes, leaves, _|
              Thread.current['nodes'] = nodes
              Thread.current['leaves'] = leaves
            end
          end
          @workers[url] = [t, b, id]
          added = true
        end
      end

      sleep 0.5 if not added
    end
  end

  def wait_for_workers
    sleep 0.5 while not @workers.empty?
  end

  def finish
    @workers.values.each { |t| t.first.join }

    @scheduler['stop'] = true
    @scheduler.join
  end
end
