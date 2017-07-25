require 'rubygems'
require 'bundler/setup'

require 'elasticsearch'
require 'pry'

require 'faker'

$client = Elasticsearch::Client.new(url: "http://elastic:changeme@127.0.0.1:9200")

# $client.transport.reload_connections!

puts $client.cluster.health

$es_index = 'join_queries_example'

$client.indices.delete(index: $es_index) rescue puts("No index yet")

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

$client.indices.create(
  index: $es_index,
  body: {
    settings: {
      index: {
        analysis: {
          analyzer: {
            trigram_english: {
              type: "custom",
              tokenizer: "standard",
              filter: [
                # "stemmer",
                "lowercase",
                "trigrams_filter",
              ]
            }
          },
          filter: {
            stemmer: {
              type: "stemmer",
              name: "english"
            },
            trigrams_filter: {
              type: "ngram",
              min_gram: 3,
              max_gram: 7,
            }
          }
        }
      }
    },
    mappings: {
      type_1: {
        properties: {
          title: {
            type: 'text',
            analyzer: 'english',
            fields: {
              trigram: {
                type: 'text',
                analyzer: 'trigram_english'
              }
            }
          },
          search_strings: {
            type: 'text',
            analyzer: 'english',
            fields: {
              trigram: {
                type: 'text',
                analyzer: 'trigram_english'
              }
            }
          }
        }
      },
      type_2: {
        _parent: {
          type: :type_1
        },
        properties: {
          title: {
            type: 'text',
            analyzer: 'english',
            fields: {
              trigram: {
                type: 'text',
                analyzer: 'trigram_english'
              }
            }
          }
        }
      },
      type_3: {
        _parent: {
          type: :type_2
        },
        properties: {
          title: {
            type: 'text',
            analyzer: 'english',
            fields: {
              trigram: {
                type: 'text',
                analyzer: 'trigram_english'
              }
            }
          }
        }
      },
    }
  }
)


def generate(from, to)
  (from..to).to_a.map do |i|
    {
      id: SecureRandom.uuid,
      title: $search_strings.sample
    }
  end
end

def generate_documents(from, to)
  #type_1
  type_1_objs =
    generate(from, to).map do |i_1|
      $client.create(
        index: $es_index,
        type: 'type_1',
        id: i_1[:id],
        body: i_1
      )
      #type_2
      i_1[:children] =
        generate(from, to).map do |i_2|
          $client.create(
            index: $es_index,
            type: 'type_2',
            id: i_2[:id],
            body: i_2,
            parent: i_1[:id]
          )
          #type_3
          i_2[:children] =
            generate(from, to).map do |i_3|
              $client.create(
                index: $es_index,
                type: 'type_3',
                id: i_3[:id],
                body: i_3,
                parent: i_2[:id]
              )
              i_3
            end
          i_2
        end
      i_1
    end

  type_1_objs.each do |i|
    strings =
      i[:children].map { |i_2| [i_2[:title]].concat(i_2[:children].map { |i_3| i_3[:title] }) }.flatten.uniq
    strings << i[:title]
    $client.update(
      index: $es_index,
      type: 'type_1',
      id: i[:id],
      body: {
        doc: i.slice(:id, :title).merge(search_strings: strings)
      }
    )
  end
end

puts 'done.'

def search
  puts 'without join queries'
  start = Time.now
  $search_strings.each do |term|
    res = $client.search(
      index: $es_index,
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
  end
  finish = Time.now
  puts "took: #{finish - start}"

  puts 'with join queries'
  start = Time.now
  $search_strings.each do |term|
    $client.search(
      index: $es_index,
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
  end
  finish = Time.now
  puts "took: #{finish - start}"
end

puts 'possible terms:'
puts $search_strings

generate_documents(1, 10)
puts 'documents count: 1110'
search
search
search

puts 'documents count: 29040'
generate_documents(11, 42)
search
search
search

generate_documents(43, 83)
puts 'documents count: 100574'
search
search
search
