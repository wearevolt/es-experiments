require 'rubygems'
require 'bundler/setup'

require 'elasticsearch'
require 'pry'

require 'faker'

$client = Elasticsearch::Client.new(url: "http://elastic:changeme@127.0.0.1:9200")

# $client.transport.reload_connections!

puts $client.cluster.health

es_index = 'join_queries_example'
#$search_strings = 20.times.map { Faker::Hipster.sentence }

$search_strings = [
  "Fixie everyday 90's tacos disrupt vhs twee.",
  "Drinking vinyl quinoa microdosing 3 wolf moon.",
  "Distillery irony kickstarter polaroid bespoke twee.",
  "Distillery cardigan organic seitan selvage fanny pack chia.",
  "Paleo etsy bicycle rights shoreditch fanny pack next level readymade pbr&b.",
  "Heirloom fixie vinegar chambray.",
  "Pork belly letterpress tousled loko waistcoat.",
  "Farm-to-table venmo vegan wes anderson park.",
  "Bushwick neutra pork belly readymade asymmetrical quinoa austin post-ironic.",
  "Messenger bag letterpress pabst before they sold out lomo gluten-free cardigan wes anderson."
]

puts 'possible terms:'
puts $search_strings

puts 'without join queries'
$search_strings.each do |term|
  start = Time.now
  res = $client.search(
    index: es_index,
    type: 'type_1',
    body: {
      query: {
        bool: {
          should: {
            match_phrase_prefix: {
              search_strings: term
            }
          }
        }
      }
    }
  )
  finish = Time.now
  puts "term: #{term}; took: #{finish - start}; found: #{res['hits']['total']}"
end

puts 'with join queries'
$search_strings.each do |term|
  start = Time.now
  res = $client.search(
    index: es_index,
    type: 'type_1',
    body: {
      query: {
        bool: {
          should: [
            {
              match_phrase_prefix: {
                title: term
              }
            },
            has_child: {
              type: 'type_2',
              query: {
                bool: {
                  should: [
                    {
                      match_phrase_prefix: {
                        title: term
                      }
                    },
                    has_child: {
                      type: 'type_3',
                      query: {
                        bool: {
                          should: [
                            {
                              match_phrase_prefix: {
                                title: term
                              }
                            }
                          ]
                        }
                      }
                    }
                  ]
                }
              }
            }
          ]
        }
      }
    }
  )
  finish = Time.now
  puts "term: #{term}; took: #{finish - start}; found: #{res['hits']['total']}"
end
