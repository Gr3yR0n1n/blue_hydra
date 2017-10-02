module BlueHydra
  module Pulse

      @@server = '127.0.0.1'
      @@port = 8244

  def self.set(server)
    @@server = server
  end

  def self.get
    @@server
  end

  def self.set(port)
    @@port = port
  end

  def self.get
    @@port
  end

      
    def send_event(key,hash)
      if BlueHydra.pulse
        SensorEvent.send_event(key,hash)
      end
    end

    def reset
      if BlueHydra.pulse ||  BlueHydra.pulse_debug

        BlueHydra.logger.info("Sending db reset to pulse")

        json_msg = JSON.generate({
          type:    "reset",
          source:  "blue-hydra",
          version: BlueHydra::VERSION,
          sync_version: BlueHydra::SYNC_VERSION,
        })

        BlueHydra::Pulse.do_send(json_msg)
      end
    end

    def hard_reset
      if BlueHydra.pulse ||  BlueHydra.pulse_debug

        BlueHydra.logger.info("Sending db hard reset to pulse")

        json_msg = JSON.generate({
          type:    "reset",
          source:  "blue-hydra",
          version: BlueHydra::VERSION,
          sync_version: "ANYTHINGBUTTHISVERSION",
        })

        BlueHydra::Pulse.do_send(json_msg)
      end
    end

    def do_send(json)
      BlueHydra::Pulse.do_debug(json) if BlueHydra.pulse_debug
      return unless BlueHydra.pulse
      begin

        @@server = BlueHydra.config["pulse_server"]
        @@port = BlueHydra.config["pulse_port"]

        # write json data to result socket
        pulse_server = @@server  #@@config["pulse_server"]
        pulse_port = @@port #@@config["pulse_port"]
        #TCPSocket.open('127.0.0.1', 8244) do |sock|
        TCPSocket.open(pulse_server, pulse_port) do |sock|
          sock.write(json)
          sock.write("\n")
          sock.flush
        end
      rescue => e
        BlueHydra.logger.warn "Unable to connect to Hermes (#{e.message}), unable to send to pulse"
      end
    end

    def do_debug(json)
      File.open("pulse_debug.log", 'a') { |file| file.puts(json) }
    end

    module_function :do_debug, :do_send, :send_event, :reset, :hard_reset
  end
end
