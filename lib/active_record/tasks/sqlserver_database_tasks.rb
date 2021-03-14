# frozen_string_literal: true

require "active_record/tasks/database_tasks"
require "shellwords"
require "ipaddr"
require "socket"

module ActiveRecord
  module Tasks
    class SQLServerDatabaseTasks
      DEFAULT_COLLATION = "SQL_Latin1_General_CP1_CI_AS"

      delegate :connection, :establish_connection, :clear_active_connections!,
               to: ActiveRecord::Base

      def initialize(configuration)
        @configuration = configuration.symbolize_keys
      end

      def create(master_established = false)
        establish_master_connection unless master_established
        connection.create_database configuration[:database], configuration.merge(collation: default_collation)
        establish_connection configuration
      rescue ActiveRecord::StatementInvalid => e
        if /database .* already exists/i === e.message
          raise DatabaseAlreadyExists
        else
          raise
        end
      end

      def drop
        establish_master_connection
        connection.drop_database configuration[:database]
      end

      def charset
        connection.charset
      end

      def collation
        connection.collation
      end

      def purge
        clear_active_connections!
        drop
        create true
      end

      def structure_dump(filename, extra_flags)
        server_arg = "-S #{Shellwords.escape(configuration[:host])}"
        server_arg += ":#{Shellwords.escape(configuration[:port])}" if configuration[:port]
        command = [
          "defncopy-ttds",
          server_arg,
          "-D #{Shellwords.escape(configuration[:database])}",
          "-U #{Shellwords.escape(configuration[:username])}",
          "-P #{Shellwords.escape(configuration[:password])}",
          "-o #{Shellwords.escape(filename)}",
        ]
        table_args = connection.tables.map { |t| Shellwords.escape(t) }
        command.concat(table_args)
        view_args = connection.views.map { |v| Shellwords.escape(v) }
        command.concat(view_args)
        raise "Error dumping database" unless Kernel.system(command.join(" "))

        dump = File.read(filename)
        dump.gsub!(/^USE .*$\nGO\n/, "")                      # Strip db USE statements
        dump.gsub!(/^GO\n/, "")                               # Strip db GO statements
        dump.gsub!(/nvarchar\(8000\)/, "nvarchar(4000)")      # Fix nvarchar(8000) column defs
        dump.gsub!(/nvarchar\(-1\)/, "nvarchar(max)")         # Fix nvarchar(-1) column defs
        dump.gsub!(/text\(\d+\)/, "text")                     # Fix text(16) column defs
        wrap_column_names(dump)
        File.open(filename, "w") { |file| file.puts dump }
      end

      def structure_load(filename, extra_flags)
        connection.execute File.read(filename)
      end

      private

      def configuration
        @configuration
      end

      def wrap_column_names(dump)
        matches = dump.scan(/\t[(,].*/)
        matches.each do |match|
          orig_match = match.dup
          nullable_regex = /((NOT )?NULL)/i
          nullable = orig_match.match(nullable_regex)&.captures&.first.dup
          match.gsub!(nullable_regex, "") if nullable
          end_parts = [nullable]
          parts = match.split
          start_parts = [parts.shift]
          end_parts << parts.pop
          new_definition = [start_parts + [parts.join(" ").prepend("[").concat("]")] + end_parts.reverse].join(" ").strip
          dump.gsub!(orig_match, new_definition)
        end

      end

      def default_collation
        configuration[:collation] || DEFAULT_COLLATION
      end

      def establish_master_connection
        establish_connection configuration.merge(database: "master")
      end
    end

    module DatabaseTasksSQLServer
      extend ActiveSupport::Concern

      module ClassMethods
        LOCAL_IPADDR = [
          IPAddr.new("192.168.0.0/16"),
          IPAddr.new("10.0.0.0/8"),
          IPAddr.new("172.16.0.0/12")
        ]

        private

        def local_database?(configuration)
          super || local_ipaddr?(configuration_host_ip(configuration))
        end

        def configuration_host_ip(configuration)
          return nil unless configuration[:host]

          Socket::getaddrinfo(configuration[:host], "echo", Socket::AF_INET)[0][3]
        end

        def local_ipaddr?(host_ip)
          return false unless host_ip

          LOCAL_IPADDR.any? { |ip| ip.include?(host_ip) }
        end
      end
    end

    DatabaseTasks.register_task %r{sqlserver}, SQLServerDatabaseTasks
    DatabaseTasks.send :include, DatabaseTasksSQLServer
  end
end
