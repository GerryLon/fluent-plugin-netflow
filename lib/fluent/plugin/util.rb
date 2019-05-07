module Fluent
  module Plugin
    class NetflowParser < Parser 
      class TemplateRegistry
        ##
        # @param logger [Logger]
        # @param ttl [Integer]
        # @param file_path [String] (optional)
        def initialize(logger, ttl, file_path=nil)
          @logger = logger
          @ttl = Integer(ttl)
          @file_path = file_path

          @mutex = Mutex.new

          @bindata_struct_cache = Vash.new
          @bindata_spec_cache = Vash.new

          do_load unless file_path.nil?
        end

        ##
        # Register a Template by name using an array of type/name tuples.
        #
        # @param key [String]: the key under which to save this template
        # @param field_tuples [Array<Array<String>>]: an array of [type,name] tuples, e.g., ["uint32","fieldName"]
        # @return [BinData::Struct]
        #
        # If a block is given, the template is yielded to the block _before_ being saved in the cache.
        #
        # @yieldparam [BinData::Struct]
        # @yieldreturn [void]
        # @yieldthrow :invalid_template : if the template is deemed invalid within the block, throwing this symbol causes
        #                                the template to not be cached.
        #
        # @threadsafe
        def register(key, field_tuples, &block)
          @mutex.synchronize do
            do_register(key, field_tuples, &block)
          end
        end

        ##
        # Fetch a Template by name
        #
        # @param key [String]
        # @return [BinData::Struct]
        #
        # @threadsafe
        def fetch(key)
          @mutex.synchronize do
            do_fetch(key)
          end
        end

        ##
        # Force persist, potentially cleaning up elements from the file-based cache that have already been evicted from
        # the memory-based cache
        def persist()
          @mutex.synchronize do
            do_persist
          end
        end

        private
        attr_reader :logger
        attr_reader :file_path

        ##
        # @see `TemplateRegistry#register(String,Array<>)`
        # @api private
        def do_register(key, field_tuples)
          template = BinData::Struct.new(:fields => field_tuples, :endian => :big)

          catch(:invalid_template) do
            yield(template) if block_given?

            @bindata_spec_cache[key, @ttl] = field_tuples
            @bindata_struct_cache[key, @ttl] = template

            do_persist

            template
          end
        end

        ##
        # @api private
        def do_load
          unless File.exists?(file_path)
            logger.warn('Template Cache does not exist', :file_path => file_path)
            return
          end

          logger.debug? and logger.debug('Loading templates from template cache', :file_path => file_path)
          file_data = File.read(file_path)
          templates_cache = JSON.parse(file_data)
          templates_cache.each do |key, fields|
            do_register(key, fields)
          end

          logger.warn('Template Cache not writable', file_path: file_path) unless File.writable?(file_path)
        rescue => e
          logger.error('Template Cache could not be loaded', :file_path => file_path, :exception => e.message)
        end

        ##
        # @see `TemplateRegistry#persist`
        # @api private
        def do_persist
          return if file_path.nil?

          logger.debug? and logger.debug('Writing templates to template cache', :file_path => file_path)

          fail('Template Cache not writable') if File.exists?(file_path) && !File.writable?(file_path)

          do_cleanup!

          templates_cache = @bindata_spec_cache

          File.open(file_path, 'w') do |file|
            file.write(templates_cache.to_json)
          end
        rescue Exception => e
          logger.error('Template Cache could not be saved', :file_path => file_path, :exception => e.message)
        end

        ##
        # @see `TemplateRegistry#cleanup`
        # @api private
        def do_cleanup!
          @bindata_spec_cache.cleanup!
          @bindata_struct_cache.cleanup!
        end

        ##
        # @see `TemplateRegistry#fetch(String)`
        # @api private
        def do_fetch(key)
          @bindata_struct_cache[key]
        end
      end
    end
  end
end