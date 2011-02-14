
require 'mechanize'
require 'sqlite3'

#
# if the directory is thought as a tree, each person is on one and only one
# leaf node so there's no need to check for duplicates
#

class PeopleStore
  def initialize
    @db = SQLite3::Database.new('people.db')
  end

  def update(leaves)
    people = update_db(leaves)
    dump_to_disk

    people
  end

  def update_db(leaves)
    people = 0

    @db.transaction do |db|
      unless leaves.empty?
        stmt = @db.prepare('INSERT OR IGNORE INTO people (url, name) VALUES(?,?)')
        leaves.each do |leaf|
          stmt.execute(leaf[:url], leaf[:title])
          people += @db.changes
        end
      end
    end

    people
  end

  def dump_to_disk
    return if people_size < 100000

    result = @db.execute('SELECT url, name FROM people')
    fname = "directory-#{Time.now.to_i}"
    raise "file exists! #{fname}" if File.exists?(fname)
    File.open(fname, 'w') do |io|
      result.each { |r| io.puts r.join("\t") }
    end
    @db.execute("DELETE FROM people")
    @db.execute("VACUUM");
  end

  def people_size
    @db.execute('SELECT COUNT(1) FROM people').first.first
  end

end

class QueueStore
  def initialize
    @db = SQLite3::Database.new('queue.db')
  end

  def reset_status
    @db.execute("UPDATE queue SET status=NULL WHERE status=0")
  end

  def next
    result = @db.execute('SELECT id, url FROM queue WHERE STATUS IS NULL ORDER BY RANDOM() LIMIT 1')
    result.first.tap do |r|
      @db.execute("UPDATE queue SET status=0 WHERE id=#{r.first}")
    end
  end

  def update(id, nodes)
    queue = 0

    @db.transaction do |db|
      unless leaves.empty?
        stmt = @db.prepare('INSERT OR IGNORE INTO people (url, name) VALUES(?,?)')
        leaves.each do |leaf|
          stmt.execute(leaf[:url], leaf[:title])
          people += @db.changes
        end
      end

      unless nodes.empty?
        stmt = @db.prepare('INSERT OR IGNORE INTO queue (url) VALUES(?)')
        nodes.each do |node|
          stmt.execute(node[:url])
          queue += @db.changes
        end
      end

      finish(id)
    end

    queue
  end

  def finish(id)
    @db.execute("UPDATE queue SET status=1 WHERE id=#{id}") unless id.nil?
  end

  def queue_size
    @db.execute('SELECT COUNT(1) FROM queue WHERE status IS NULL').first.first
  end

end

def download(agent, url)
  retries = 0
  page = nil
  begin
    print url
    return agent.get(url)
  rescue Errno::ECONNREFUSED, Timeout::Error, Net::HTTPInternalServerError,
  Errno::ETIMEDOUT
    retries += 1
    puts " Error: #{$!.message}"
    if retries > 3
      puts "Too many retries"
      exit
    end
    sleep 15
    retry
  end
end

def parse(page)
  links = page.links.select { |link| link.href =~ %r{/people/.} }
  links.map! { |link| { :url => link.href, :title => link.text } }

  links.partition { |link| link[:url] =~ %r{/directory/} }
end

class Store
  def initialize
    @queue = QueueStore.new
    @people = PeopleStore.new
  end

  def reset_status
    @queue.reset_status
  end

  def update(id, nodes, leaves)
    [ @queue.update(id, nodes), @people.update(leaves) ]
  end
end

$db = Store.new
$db.reset_status



n = 0
while true do
  result = $db.next
  if result.nil?
    puts "No more URLs"
    break
  end

  id = result.first
  url = result.last
    
  agent = Mechanize.new
  agent.user_agent_alias = 'Windows IE 7'

  page = download(agent, url)
  nodes, leaves = parse(page)

  people, queue = $db.update(id, nodes, leaves)

  buf = "\t"
  buf += "#{queue}/" if queue != nodes.size
  buf += "#{nodes.size}\t"
  buf += "#{people}/" if people != leaves.size
  buf += "#{leaves.size}"
  puts buf

  n += 1
  puts "queue: #{$db.queue_size}" if n % 50 == 0
end
