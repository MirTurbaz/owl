class BasicDecorator < SimpleDelegator
  class << self
    def wrap_collection(items)
      items.map { |item| new(item) }
    end

    protected
      def decorates_associated(*association_names, with: name.demodulize)
        association_names.each do |association_name|
          association = module_parent.reflections[association_name.to_s]
          association_class = (association.options[:class_name] || association.name.to_s.camelize.singularize).safe_constantize
          decorator_class = association_class.const_get(with)

          wrap_method =
            case association.class.to_s
              when 'ActiveRecord::Reflection::BelongsToReflection', 'ActiveRecord::Reflection::HasOneReflection'
                :new
              when 'ActiveRecord::Reflection::HasManyReflection', 'ActiveRecord::Reflection::HasAndBelongsToManyReflection'
                :wrap_collection
              else
                raise NotImplementedError
            end

          define_method(association_name) do
            instance_eval("@#{association_name} ||= #{decorator_class}.#{wrap_method}(super())")
          end
        end
      end
  end

  def initialize(object)
    object_name = object.class.name.underscore
    define_singleton_method(object_name) { object }
    super(object)
  end

  # @todo избавиться от дублирования кода из Decoratable
  def decorate(*args)
    decorators = args.reduce([]) { |arr, decorator| decorator == :presenter ? arr << decorator : arr.unshift(decorator) }
    decorators.reduce(self) do |result, arg|
      next arg.reduce(result) { |memo, pair| decorator_class(pair.first).new(memo, *pair.last) } if arg.respond_to?(:to_h)
      decorator_class(arg).new(result)
    end
  end

  def is_a?(arg)
    super || __getobj__.is_a?(arg)
  end

  def kind_of?(arg)
    super || __getobj__.kind_of?(arg)
  end

  def class
    __getobj__.class
  end
end
