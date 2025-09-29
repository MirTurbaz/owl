class Settings
  delegate :each, :map, to: :to_h

  # @param [AbstractAdapter] connection соединение с БД
  # @param [String] table название таблицы
  def initialize(connection, table: 'settings')
    @connection, @table = connection, table
    @cache = {}
    @all_cached = false
  end

  # Получает значение из таблицы настроек по ключу
  # @param [String, Symbol] key
  # @return [Object]
  def [](key)
    return @cache[key] if @cache.key?(key)

    query = <<-SQL
      SELECT value FROM #{@table} WHERE key = #{@connection.quote(key)}
    SQL
    result = @connection.execute(query).to_a.first
    value = result ? YAML.load(result['value']) : nil
    @cache[key] = value
    value
  end

  # Задаёт значение соответствующему ключу в таблице настроек
  # @param [String, Symbol] key
  # @param [Object] value
  def []=(key, value)
    quoted_key = @connection.quote(key)
    quoted_value = @connection.quote(value.to_yaml)

    query =
      if key_exists?(key)
        <<-SQL
          UPDATE #{@table} SET value = #{quoted_value} WHERE #{@table}.key = #{quoted_key}
        SQL
      else
        <<-SQL
          INSERT INTO #{@table} (key, value) VALUES (#{quoted_key}, #{quoted_value})
        SQL
      end
    # TODO: вернуть, когда обновим постгрес
    # query = <<-SQL
    #   INSERT INTO #{@table} (key, value)
    #   VALUES (#{quoted_key}, #{quoted_value})
    #   ON CONFLICT (key) DO
    #   UPDATE SET value = #{quoted_value} WHERE #{@table}.key = #{quoted_key}
    # SQL
    @connection.execute(query)

    @cache[key] = value
    @all_cached = false
  end

  def to_h
    return @all_cache if @all_cached

    query = <<-SQL
      SELECT key, value FROM #{@table}
    SQL
    result = @connection.execute(query).to_a

    @all_cache = result.map { |setting|
      key = setting['key']
      value = YAML.load(setting['value'])

      @cache[key] = value

      { key => value }
    }.reduce(:merge).with_indifferent_access

    @all_cached = true
    @all_cache
  end

  private
    def key_exists?(key)
      return @cache.key?(key) if @all_cached

      quoted_key = @connection.quote(key)

      query = <<-SQL
        SELECT TRUE FROM settings WHERE key = #{quoted_key}
      SQL

      !!@connection.execute(query).to_a.first
    end
end
