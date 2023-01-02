// Copyright 2021, 2022 Peter Dimov.
// Copyright 2022-2023 Joaquin M Lopez Munoz.
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define _SILENCE_CXX17_OLD_ALLOCATOR_MEMBERS_DEPRECATION_WARNING
#define _SILENCE_CXX20_CISO646_REMOVED_WARNING

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/regex.hpp>
#include <vector>
#include <memory>
#include <cstdint>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <fstream>
#include <string_view>
#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include <shared_mutex>
#include "rw_spinlock.hpp"
#include "cfoa.hpp"
#include "cuckoohash_map.hh"
#include "oneapi/tbb/concurrent_hash_map.h"

int const Th = 8; // number of threads
int const Sh = Th * Th; // number of shards

using namespace std::chrono_literals;

static void print_time( std::chrono::steady_clock::time_point & t1, char const* label, std::size_t s, std::size_t size )
{
    auto t2 = std::chrono::steady_clock::now();

    std::cout << label << ": " << ( t2 - t1 ) / 1ms << " ms (s=" << s << ", size=" << size << ")\n";

    t1 = t2;
}

static std::vector<std::string> words;

static void init_words()
{
#if SIZE_MAX > UINT32_MAX

    char const* fn = "enwik9"; // http://mattmahoney.net/dc/textdata

#else

    char const* fn = "enwik8"; // ditto

#endif

    auto t1 = std::chrono::steady_clock::now();

    std::ifstream is( fn );
    std::string in( std::istreambuf_iterator<char>( is ), std::istreambuf_iterator<char>{} );

    boost::regex re( "[a-zA-Z]+");
    boost::sregex_token_iterator it( in.begin(), in.end(), re, 0 ), end;

    words.assign( it, end );

    auto t2 = std::chrono::steady_clock::now();

    std::cout << fn << ": " << words.size() << " words, " << ( t2 - t1 ) / 1ms << " ms\n\n";
}

struct ufm_single_threaded
{
    boost::unordered_flat_map<std::string_view, std::size_t> map;

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::size_t s = 0;

        for( auto const& word: words )
        {
            ++map[ word ];
            ++s;
        }

        print_time( t1, "Word count", s, map.size() );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::size_t s = 0;

        for( auto const& word: words )
        {
            std::string_view w2( word );
            w2.remove_prefix( 1 );

            s += map.contains( w2 );
        }

        print_time( t1, "Contains", s, map.size() );

        std::cout << std::endl;
    }
};

struct ufm_mutex
{
    alignas(64) boost::unordered_flat_map<std::string_view, std::size_t> map;
    alignas(64) std::mutex mtx;

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        size_t s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::lock_guard<std::mutex> lock( mtx );

                    ++map[ words[j] ];
                    ++s;
                }
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Word count", s, map.size() );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        size_t s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::lock_guard<std::mutex> lock( mtx );

                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    s += map.contains( w2 );
                }
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Contains", s, map.size() );

        std::cout << std::endl;
    }
};

struct ufm_rwlock
{
    alignas(64) boost::unordered_flat_map<std::string_view, std::size_t> map;
    alignas(64) std::shared_mutex mtx;

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::lock_guard<std::shared_mutex> lock( mtx );

                    ++map[ words[j] ];
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Word count", s, map.size() );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::shared_lock<std::shared_mutex> lock(mtx);

                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    s2 += map.contains( w2 );
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Contains", s, map.size() );

        std::cout << std::endl;
    }
};

struct ufm_rw_spinlock
{
    alignas(64) boost::unordered_flat_map<std::string_view, std::size_t> map;
    alignas(64) rw_spinlock mtx;

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::lock_guard<rw_spinlock> lock( mtx );

                    ++map[ words[j] ];
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Word count", s, map.size() );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::shared_lock<rw_spinlock> lock(mtx);

                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    s2 += map.contains( w2 );
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Contains", s, map.size() );

        std::cout << std::endl;
    }
};

template<class Mtx> struct sync_map
{
    alignas(64) boost::unordered_flat_map<std::string_view, std::size_t> map;
    alignas(64) Mtx mtx;
};

struct ufm_sharded_mutex
{
    sync_map<std::mutex> sync[ Sh ];

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    auto const& word = words[ j ];

                    std::size_t hash = boost::hash<std::string_view>()( word );
                    std::size_t shard = hash % Sh;

                    std::lock_guard<std::mutex> lock( sync[ shard ].mtx );

                    ++sync[ shard ].map[ word ];
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Word count", s, n );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    std::size_t hash = boost::hash<std::string_view>()( w2 );
                    std::size_t shard = hash % Sh;

                    std::lock_guard<std::mutex> lock( sync[ shard ].mtx );

                    s2 += sync[ shard ].map.contains( w2 );
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Contains", s, n );

        std::cout << std::endl;
    }
};

//

struct prehashed
{
    std::string_view x;
    std::size_t h;

    explicit prehashed( std::string_view x_ ): x( x_ ), h( boost::hash<std::string_view>()( x_ ) ) { }

    operator std::string_view () const
    {
        return x;
    }

    friend bool operator==( prehashed const& x, prehashed const& y )
    {
        return x.x == y.x;
    }

    friend bool operator==( prehashed const& x, std::string_view y )
    {
        return x.x == y;
    }

    friend bool operator==( std::string_view x, prehashed const& y )
    {
        return x == y.x;
    }
};

template<>
struct boost::hash< prehashed >
{
    using is_transparent = void;

    std::size_t operator()( prehashed const& x ) const
    {
        return x.h;
    }

    std::size_t operator()( std::string_view x ) const
    {
        return boost::hash<std::string_view>()( x );
    }
};

template<class Mtx> struct sync_map_prehashed
{
    alignas(64) boost::unordered_flat_map< std::string_view, std::size_t, boost::hash<prehashed>, std::equal_to<> > map;
    alignas(64) Mtx mtx;
};

struct ufm_sharded_mutex_prehashed
{
    sync_map_prehashed<std::mutex> sync[ Sh ];

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view word = words[ j ];

                    prehashed x( word );
                    std::size_t shard = x.h % Sh;

                    std::lock_guard<std::mutex> lock( sync[ shard ].mtx );

                    ++sync[ shard ].map[ x ];
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Word count", s, n );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    prehashed x( w2 );
                    std::size_t shard = x.h % Sh;

                    std::lock_guard<std::mutex> lock( sync[ shard ].mtx );

                    s2 += sync[ shard ].map.contains( x );
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Contains", s, n );

        std::cout << std::endl;
    }
};

//

struct ufm_sharded_rwlock
{
    sync_map<std::shared_mutex> sync[ Sh ];

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    auto const& word = words[ j ];

                    std::size_t hash = boost::hash<std::string_view>()( word );
                    std::size_t shard = hash % Sh;

                    std::lock_guard<std::shared_mutex> lock( sync[ shard ].mtx );

                    ++sync[ shard ].map[ word ];
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Word count", s, n );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    std::size_t hash = boost::hash<std::string_view>()( w2 );
                    std::size_t shard = hash % Sh;

                    std::shared_lock<std::shared_mutex> lock( sync[ shard ].mtx );

                    s2 += sync[ shard ].map.contains( w2 );
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Contains", s, n );

        std::cout << std::endl;
    }
};

struct ufm_sharded_rwlock_prehashed
{
    sync_map_prehashed<std::shared_mutex> sync[ Sh ];

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view word = words[ j ];

                    prehashed x( word );
                    std::size_t shard = x.h % Sh;

                    std::lock_guard<std::shared_mutex> lock( sync[ shard ].mtx );

                    ++sync[ shard ].map[ x ];
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Word count", s, n );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    prehashed x( w2 );
                    std::size_t shard = x.h % Sh;

                    std::shared_lock<std::shared_mutex> lock( sync[ shard ].mtx );

                    s2 += sync[ shard ].map.contains( x );
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Contains", s, n );

        std::cout << std::endl;
    }
};

struct ufm_sharded_rw_spinlock
{
    sync_map<rw_spinlock> sync[ Sh ];

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    auto const& word = words[ j ];

                    std::size_t hash = boost::hash<std::string_view>()( word );
                    std::size_t shard = hash % Sh;

                    std::lock_guard<rw_spinlock> lock( sync[ shard ].mtx );

                    ++sync[ shard ].map[ word ];
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Word count", s, n );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    std::size_t hash = boost::hash<std::string_view>()( w2 );
                    std::size_t shard = hash % Sh;

                    std::shared_lock<rw_spinlock> lock( sync[ shard ].mtx );

                    s2 += sync[ shard ].map.contains( w2 );
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Contains", s, n );

        std::cout << std::endl;
    }
};

struct ufm_sharded_rw_spinlock_prehashed
{
    sync_map_prehashed<rw_spinlock> sync[ Sh ];

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view word = words[ j ];

                    prehashed x( word );
                    std::size_t shard = x.h % Sh;

                    std::lock_guard<rw_spinlock> lock( sync[ shard ].mtx );

                    ++sync[ shard ].map[ x ];
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Word count", s, n );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    prehashed x( w2 );
                    std::size_t shard = x.h % Sh;

                    std::shared_lock<rw_spinlock> lock( sync[ shard ].mtx );

                    s2 += sync[ shard ].map.contains( x );
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Sh; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Contains", s, n );

        std::cout << std::endl;
    }
};

//

struct ufm_sharded_isolated
{
    struct
    {
        alignas(64) boost::unordered_flat_map<std::string_view, std::size_t> map;
    }
    sync[ Th ];

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, &s]{

                std::size_t s2 = 0;

                for( std::size_t j = 0; j < words.size(); ++j )
                {
                    auto const& word = words[ j ];

                    std::size_t hash = boost::hash<std::string_view>()( word );
                    std::size_t shard = hash % Th;

                    if( shard == i )
                    {
                        ++sync[ i ].map[ word ];
                        ++s2;
                    }
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Th; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Word count", s, n );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, &s]{

                std::size_t s2 = 0;

                for( std::size_t j = 0; j < words.size(); ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    std::size_t hash = boost::hash<std::string_view>()( w2 );
                    std::size_t shard = hash % Th;

                    if( shard == i )
                    {
                        s2 += sync[ i ].map.contains( w2 );
                    }
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Th; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Contains", s, n );

        std::cout << std::endl;
    }
};

struct ufm_sharded_isolated_prehashed
{
    struct
    {
        alignas(64) boost::unordered_flat_map<std::string_view, std::size_t, boost::hash<prehashed>, std::equal_to<>> map;
    }
    sync[ Th ];

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, &s]{

                std::size_t s2 = 0;

                for( std::size_t j = 0; j < words.size(); ++j )
                {
                    std::string_view word = words[ j ];

                    prehashed x( word );
                    std::size_t shard = x.h % Th;

                    if( shard == i )
                    {
                        ++sync[ i ].map[ x ];
                        ++s2;
                    }
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Th; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Word count", s, n );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, &s]{

                std::size_t s2 = 0;

                for( std::size_t j = 0; j < words.size(); ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    prehashed x( w2 );
                    std::size_t shard = x.h % Th;

                    if( shard == i )
                    {
                        s2 += sync[ i ].map.contains( x );
                    }
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        std::size_t n = 0;

        for( std::size_t i = 0; i < Th; ++i )
        {
            n += sync[ i ].map.size();
        }

        print_time( t1, "Contains", s, n );

        std::cout << std::endl;
    }
};

template<typename Key,typename T>
struct map_policy
{
  using key_type=Key;
  using raw_key_type=typename std::remove_const<Key>::type;
  using raw_mapped_type=typename std::remove_const<T>::type;

  using init_type=std::pair<raw_key_type,raw_mapped_type>;
  using moved_type=std::pair<raw_key_type&&,raw_mapped_type&&>;
  using value_type=std::pair<const Key,T>;
  using element_type=value_type;

  static value_type& value_from(element_type& x)
  {
    return x;
  }

  template <class K,class V>
  static const raw_key_type& extract(const std::pair<K,V>& kv)
  {
    return kv.first;
  }

  static moved_type move(value_type& x)
  {
    return{
      std::move(const_cast<raw_key_type&>(x.first)),
      std::move(const_cast<raw_mapped_type&>(x.second))
    };
  }

  template<typename Allocator,typename... Args>
  static void construct(Allocator& al,element_type* p,Args&&... args)
  {
    boost::allocator_traits<Allocator>::
      construct(al,p,std::forward<Args>(args)...);
  }

  template<typename Allocator>
  static void destroy(Allocator& al,element_type* p)noexcept
  {
    boost::allocator_traits<Allocator>::destroy(al,p);
  }
};

struct ufm_concurrent_foa
{
    boost::unordered::detail::cfoa::table<
        map_policy<std::string_view, std::size_t>,
        boost::hash<std::string_view>, std::equal_to<std::string_view>,
        std::allocator<std::pair<const std::string_view,int>>> map;

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    map.try_emplace(
                        []( auto& x, bool ){ ++x.second; },
                        words[j], 0 );
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Word count", s, map.size() );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    map.find(w2, [&]( auto& ){ ++s2; } );
                }

                s += s2;

            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Contains", s, map.size() );

        std::cout << std::endl;
    }
};

struct libcuckoo_cuckoohash_map
{
    libcuckoo::cuckoohash_map<
        std::string_view, std::size_t,
        boost::hash<std::string_view>, std::equal_to<std::string_view>,
        std::allocator<std::pair<const std::string_view,int>>> map;

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    map.uprase_fn(
                        words[j],
                        []( auto& x){ ++x; return false; },
                        0 );
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Word count", s, map.size() );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    map.find_fn(w2, [&]( auto& ){ ++s2; } );
                }

                s += s2;

            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Contains", s, map.size() );

        std::cout << std::endl;
    }
};

struct tbb_concurrent_hash_map
{
    struct hash_compare
    {
        std::size_t hash( std::string_view const& x ) const
        {
            return boost::hash<std::string_view>()( x );
        }

        bool equal( std::string_view const& x, std::string_view const& y ) const
        {
            return x == y;
        }
    };

    using map_type = tbb::concurrent_hash_map <
        std::string_view, std::size_t, hash_compare >;
    using accessor = map_type::accessor;
    
    map_type map;

    BOOST_NOINLINE void test_word_count( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    accessor acc;

                    map.emplace( acc, words[j], 0 );
                    ++acc->second;
                    ++s2;
                }

                s += s2;
            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Word count", s, map.size() );

        std::cout << std::endl;
    }

    BOOST_NOINLINE void test_contains( std::chrono::steady_clock::time_point & t1 )
    {
        std::atomic<std::size_t> s = 0;

        std::thread th[ Th ];

        std::size_t m = words.size() / Th;

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ] = std::thread( [this, i, m, &s]{

                std::size_t s2 = 0;

                std::size_t start = i * m;
                std::size_t end = i == Th-1? words.size(): (i + 1) * m;

                for( std::size_t j = start; j < end; ++j )
                {
                    std::string_view w2( words[j] );
                    w2.remove_prefix( 1 );

                    s2 += map.count( w2 );
                }

                s += s2;

            });
        }

        for( std::size_t i = 0; i < Th; ++i )
        {
            th[ i ].join();
        }

        print_time( t1, "Contains", s, map.size() );

        std::cout << std::endl;
    }
};

//

struct record
{
    std::string label_;
    long long time_;
};

static std::vector<record> times;

template<class Map> BOOST_NOINLINE void test( char const* label )
{
    std::cout << label << ":\n\n";

    Map map;

    auto t0 = std::chrono::steady_clock::now();
    auto t1 = t0;

    record rec = { label, 0 };

    map.test_word_count( t1 );
    map.test_contains( t1 );

    auto tN = std::chrono::steady_clock::now();
    std::cout << "Total: " << ( tN - t0 ) / 1ms << " ms\n\n";

    rec.time_ = ( tN - t0 ) / 1ms;
    times.push_back( rec );
}

//

int main()
{
    init_words();

    test<ufm_single_threaded>( "boost::unordered_flat_map, single threaded" );
    test<ufm_mutex>( "boost::unordered_flat_map, mutex" );
    test<ufm_rwlock>( "boost::unordered_flat_map, rwlock" );
    test<ufm_rw_spinlock>( "boost::unordered_flat_map, rw_spinlock" );
    test<ufm_sharded_mutex>( "boost::unordered_flat_map, sharded mutex" );
    test<ufm_sharded_mutex_prehashed>( "boost::unordered_flat_map, sharded mutex, prehashed" );
    test<ufm_sharded_rwlock>( "boost::unordered_flat_map, sharded rwlock" );
    test<ufm_sharded_rwlock_prehashed>( "boost::unordered_flat_map, sharded rwlock, prehashed" );
    test<ufm_sharded_rw_spinlock>( "boost::unordered_flat_map, sharded rw_spinlock" );
    test<ufm_sharded_rw_spinlock_prehashed>( "boost::unordered_flat_map, sharded rw_spinlock, prehashed" );
    test<ufm_sharded_isolated>( "boost::unordered_flat_map, sharded isolated" );
    test<ufm_sharded_isolated_prehashed>( "boost::unordered_flat_map, sharded isolated, prehashed" );
    test<ufm_concurrent_foa>( "concurrent foa" );
    test<libcuckoo_cuckoohash_map>( "libcuckoo::cuckoohash_map" );
    test<tbb_concurrent_hash_map>( "tbb::concurrent_hash_map" );

    std::cout << "---\n\n";

    for( auto const& x: times )
    {
        std::cout << std::setw( 60 ) << ( x.label_ + ": " ) << std::setw( 5 ) << x.time_ << " ms\n";
    }
}
