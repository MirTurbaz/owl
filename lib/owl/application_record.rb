require_relative 'destroy_restriction'

class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true

  if Rails.configuration.database_configuration[Rails.env]["primary_replica"].present?
    connects_to database: { writing: :primary, reading: :primary_replica }
  end

  include DestroyRestriction

  class << self
    # Список атрибутов модели
    # @param [Hash] options
    #   @option options [Boolean] :recursive включать ли nested_attributes
    # @return [Array]
    def attribute_names(options = {})
      return super() if options[:recursive].blank?
      generate_attributes_list(self)
    end

    private
      # Рекурсивно генерирует список атрибутов класса модели
      # @param [Class] model_class класс сущности
      # @return [Array]
      def generate_attributes_list(model_class)
        nested_attributes = model_class.nested_attributes_options.keys.inject({}) do |hash, relation|
          association = model_class.reflections[relation.to_s]
          relation_class = (association.options[:class_name] || association.name.to_s.camelize.singularize).safe_constantize
          next {} unless relation_class.present?
          hash["#{relation}_attributes"] = generate_attributes_list(relation_class) | [:_destroy]
          hash
        end

        model_class.attribute_names.map(&:to_sym) + [nested_attributes] - [:created_at, :updated_at]
      end
  end

  # Получить список пар атрибутов экземпляра модели
  # @param [Array<Symbol>] exclude список имен атрибутов экземпляра модели, которые следует исключить из выборки
  # @return [Hash<String, Object>]
  def attributes_sanitized(exclude: %i[id created_at updated_at])
    excluded_attributes = exclude.map(&:to_s)
    filter = proc { |attr_name| attr_name.in?(excluded_attributes) }

    attributes
      .slice(*self
                .class
                .attribute_names
                .reject(&filter)
    )
  end
end
