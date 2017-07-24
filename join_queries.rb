require 'rubygems'
require 'bundler/setup'

require 'elasticsearch'
require 'pry'

require 'faker'

$client = Elasticsearch::Client.new(url: "http://elastic:changeme@127.0.0.1:9200")

# $client.transport.reload_connections!

puts $client.cluster.health

es_index = 'join_queries_example'
$client.indices.delete(index: es_index) rescue puts("No index yet")

$search_strings = 20.times.map { Faker::Hipster.word }

res = $client.indices.create(
  index: es_index,
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

binding.pry

def generate
  (1..20).to_a.map do |i|
    {
      id: SecureRandom.uuid,
      title: $search_strings.sample
    }
  end
end

puts 'generating...'
#type_1
type_1_objs =
  generate.map do |i_1|
    $client.create(
      index: es_index,
      type: 'type_1',
      id: i_1[:id],
      body: i_1
    )
    #type_2
    i_1[:children] =
      generate.map do |i_2|
        $client.create(
          index: es_index,
          type: 'type_2',
          id: i_2[:id],
          body: i_2,
          parent: i_1[:id]
        )
        #type_3
        i_2[:children] =
          generate.map do |i_3|
            $client.create(
              index: es_index,
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
  binding.pry
  $client.update(
    index: es_index,
    type: 'type_1',
    id: i[:id],
    body: {
      doc: i.slice(:id, :title).merge(search_strings: strings)
    }
  )
end
puts 'done.'

puts 'possible terms:'
puts $search_strings.join(' ')

terms = 10.times.map { $search_strings.sample }
puts 'without join queries'
terms.each do |term|
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
  puts "term: #{term}; took: #{res['took']}; finded: #{res['hits']['total']}"
end

puts 'with join queries'
terms.each do |term|
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
  puts "term: #{term}; took: #{res['took']}; finded: #{res['hits']['total']}"
end
