// Copyright 2021, 2022 Peter Dimov.
// Copyright 2022-2023 Joaquin M Lopez Munoz.
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define _SILENCE_CXX17_OLD_ALLOCATOR_MEMBERS_DEPRECATION_WARNING
#define _SILENCE_CXX20_CISO646_REMOVED_WARNING

#include <boost/unordered/concurrent_flat_map.hpp>
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
#include "oneapi/tbb/concurrent_hash_map.h"

#if !defined(NUM_THREADS)
# define NUM_THREADS 48
#endif

using namespace std::chrono_literals;

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

//

// map types

using cfm_map_type = boost::concurrent_flat_map<std::string_view, std::size_t>;

struct tbb_hash_compare
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

using tbb_map_type = tbb::concurrent_hash_map<std::string_view, std::size_t, tbb_hash_compare>;

// map operations

inline void increment_element( cfm_map_type& map, std::string_view key )
{
    map.emplace_or_visit( key, 1, []( auto& x ){ ++x.second; } );
}

inline bool contains_element( cfm_map_type const& map, std::string_view key )
{
    return map.contains( key );
}

inline void increment_element( tbb_map_type& map, std::string_view key )
{
    tbb_map_type::accessor acc;

    map.emplace( acc, key, 0 );
    ++acc->second;
}

inline bool contains_element( tbb_map_type const& map, std::string_view key )
{
    return map.count( key ) != 0;
}

//

template<class Map> BOOST_NOINLINE void test_word_count( Map& map, std::size_t Th )
{
    auto t1 = std::chrono::steady_clock::now();

    std::atomic<std::size_t> s = 0;

    std::vector<std::thread> th( Th );

    std::size_t m = words.size() / Th;

    for( std::size_t i = 0; i < Th; ++i )
    {
        th[ i ] = std::thread( [&map, Th, i, m, &s]{

            std::size_t s2 = 0;

            std::size_t start = i * m;
            std::size_t end = i == Th-1? words.size(): (i + 1) * m;

            for( std::size_t j = start; j < end; ++j )
            {
                increment_element( map, words[j] );
                ++s2;
            }

            s += s2;
        });
    }

    for( std::size_t i = 0; i < Th; ++i )
    {
        th[ i ].join();
    }

    auto t2 = std::chrono::steady_clock::now();

    std::cout << ";" << ( t2 - t1 ) / 1ms << ";" << s;
}

//

template<class Map> BOOST_NOINLINE void test( std::size_t Th )
{
    Map map;
    test_word_count( map, Th );
}

//

int main()
{
    init_words();

    std::cout << "NUM_THREADS=" << NUM_THREADS << "\n\n";
    std::cout << "#threads;boost::concurrent_hash_map time;boost::concurrent_hash_map checksum;tbb::concurrent_hash_map time;tbb::concurrent_hash_map checksum" << std::endl;

    for( std::size_t Th = 1; Th <= NUM_THREADS; ++Th)
    {
        std::cout << Th;

        test<cfm_map_type>( Th );
        test<tbb_map_type>( Th );

        std::cout << std::endl;
    }
}
