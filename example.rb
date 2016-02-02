require "stomp"
require "byebug"
require "zlib"
require "stringio"

class NrPollerPoller

  # Initialize the poller

  def initialize

    @hostname = 'datafeeds.nationalrail.co.uk'
    @username = 'd3user'
    @password = 'd3password'

    puts "Stomp consumer for Network Rail Open Data Distribution Service"

  end


  # Connect to the service and process messages

  def run

    client_headers = { "accept-version" => "1.1", "heart-beat" => "5000,10000", "client-id" => Socket.gethostname, "host" => @hostname }
    client_hash = { :hosts => [ { :login => @username, :passcode => @password, :host => @hostname, :port => 61613 } ], :connect_headers => client_headers }

    client = Stomp::Client.new(client_hash)

    # Check we have connected successfully

    raise "Connection failed" unless client.open?
    raise "Connect error: #{client.connection_frame().body}" if client.connection_frame().command == Stomp::CMD_ERROR
    raise "Unexpected protocol level #{client.protocol}" unless client.protocol == Stomp::SPL_11

    puts "Connected to #{client.connection_frame().headers['server']} server with STOMP #{client.connection_frame().headers['version']}"


    # Subscribe to the RTPPM topic and process messages

    client.subscribe("/queue/D3250cbf13-330d-4c70-b4d1-af92a67d8c1b") do |msg|
      # byebug
      gz = Zlib::GzipReader.new(StringIO.new(msg.body.to_s))
      string = gz.read
      puts string
      # File.open("message", 'w') { |file| file.write(string) }
      client.acknowledge(msg, msg.headers)

    end

    client.join


    # We will probably never end up here

    client.close
    puts "Client close complete"

  end

end

e = NrPollerPoller.new
e.run
