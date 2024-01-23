require_relative '../owl_application_record'

# == Schema Information
#
# Table name: view_counters
#
#  id              :integer          not null, primary key
#  item_id         :integer
#  item_type       :string
#  date            :date             not null
#  views_count     :integer          default(0), not null
#  bot_views_count :integer          default(0), not null
#

class ViewCounter < OwlApplicationRecord
  belongs_to :item, polymorphic: true

  validates_uniqueness_of :item_id, scope: [:item_type, :date]
  validates_presence_of :item_id, :item_type
end
