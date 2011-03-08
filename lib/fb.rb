
$LOAD_PATH << File.join(File.dirname(__FILE__))

require 'fileutils'
require 'store'
require 'download'
require 'compress'

$stdout.sync = true
Thread.abort_on_exception = true

#
# if the directory is thought as a tree, each person is on one and only one
# leaf node so there's no need to check for duplicates
#

class Harvester
  def initialize(db)
    @downloader = Downloader.new
    @db = db
  end

  def run
    n = 0
    almost_none = false
    while true do
      result = @db.next
      if result.nil?
        if almost_none
          puts "No more URLs"
          break
        end

        almost_none = true
        @downloader.wait_for_workers
        next
      end

      id = result.first
      url = result.last
        
      @downloader.get(url, id) do |nodes, leaves, url, id|
        queue, people = @db.update(id, nodes, leaves)
        print_stats(url, queue, nodes, people, leaves)
        n += 1
        if n > 50
          n = 0
          puts "queue: #{@db.queue_size}"
        end
      end
    end
    @downloader.finish

    # no more URLs to process, dump what is left in the people db
    @db.force_dump if @db.queue_size == 0
  end

  protected

  def print_stats(url, queue, nodes, people, leaves)
    buf = "#{url}\t"
    buf += "#{queue}/" if queue != nodes.size
    buf += "#{nodes.size}\t"
    buf += "#{people}/" if people != leaves.size
    buf += "#{leaves.size}"
    puts buf
  end
end

