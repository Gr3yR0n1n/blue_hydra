module BlueHydra
  module Strike
    def send_event(key,hash)
      if BlueHydra.strike
        SensorEvent.send_event(key,hash)
      end
    end

    def reset
      if BlueHydra.strike #||  BlueHydra.pulse_debug

        BlueHydra.logger.info("Sending db reset to strike")

        json_msg = JSON.generate({
          type:    "reset",
          source:  "blue-hydra",
          version: BlueHydra::VERSION,
          sync_version: BlueHydra::SYNC_VERSION,
        })

        BlueHydra::Strike.do_send(json_msg)
      end
    end

    def hard_reset
      if BlueHydra.strike #||  BlueHydra.pulse_debug

        BlueHydra.logger.info("Sending db hard reset to strike")

        json_msg = JSON.generate({
          type:    "reset",
          source:  "blue-hydra",
          version: BlueHydra::VERSION,
          sync_version: "ANYTHINGBUTTHISVERSION",
        })

        BlueHydra::Strike.do_send(json_msg)
      end
    end

    def do_send(json)
      #BlueHydra::Strike.do_debug(json) if BlueHydra.pulse_debug
      #return unless BlueHydra.strike
      begin
        # write json data to result socket
        TCPSocket.open('127.0.0.1', 8244) do |sock|
          sock.write(json)
          sock.write("\n")
          sock.flush
        end
      rescue => e
        BlueHydra.logger.warn "Unable to connect to Striker (#{e.message}), unable to send to strike"
      end
    end

    def do_debug(json)
      File.open("strike_debug.log", 'a') { |file| file.puts(json) }
    end

    module_function :do_debug, :do_send, :send_event, :reset, :hard_reset
  end
end
