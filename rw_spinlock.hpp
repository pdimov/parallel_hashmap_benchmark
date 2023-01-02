#ifndef RW_SPINLOCK_HPP_INCLUDED
#define RW_SPINLOCK_HPP_INCLUDED

// Copyright 2023 Peter Dimov
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#include <boost/smart_ptr/detail/sp_thread_pause.hpp>
#include <boost/smart_ptr/detail/sp_thread_sleep.hpp>
#include <atomic>
#include <cstdint>

class rw_spinlock
{
private:

    // bit 31: locked exclusive
    // bit 30: writer pending
    // bit 29..: reader lock count

    std::atomic<std::uint32_t> state_ = {};

private:

    static void yield_k( unsigned k ) noexcept
    {
        if( k < 49152 )
        {
            boost::detail::sp_thread_pause();
        }
        else
        {
            boost::detail::sp_thread_sleep();
        }
    }

public:

    bool try_lock_shared() noexcept
    {
        std::uint32_t st = state_.load( std::memory_order_relaxed );

        if( st >= 0x3FFF'FFFF )
        {
            // either bit 31 set, bit 30 set, or reader count is max
            return false;
        }

        std::uint32_t newst = st + 1;
        return state_.compare_exchange_strong( st, newst, std::memory_order_acquire, std::memory_order_relaxed );
    }

    void lock_shared() noexcept
    {
        for( unsigned k = 0;; ++k )
        {
            std::uint32_t st = state_.load( std::memory_order_relaxed );

            if( st < 0x3FFF'FFFF )
            {
                std::uint32_t newst = st + 1;
                if( state_.compare_exchange_strong( st, newst, std::memory_order_acquire, std::memory_order_relaxed ) ) break;
            }

            yield_k( k );
        }
    }

    void unlock_shared() noexcept
    {
        // pre: locked shared, not locked exclusive

        std::uint32_t st = state_.fetch_sub( 1, std::memory_order_release ) - 1;

        for( ;; )
        {
            // if we were the last reader, we need to clear the writer pending bit

            if( st != 0x4000'0000 ) break;

            std::uint32_t newst = 0;
            if( state_.compare_exchange_weak( st, newst, std::memory_order_relaxed, std::memory_order_relaxed ) ) break;
        }
    }

    bool try_lock() noexcept
    {
        std::uint32_t st = state_.load( std::memory_order_relaxed );

        if( st & 0x8000'0000 )
        {
            // locked exclusive
            return false;
        }

        if( st & 0x3FFF'FFFF )
        {
            // locked shared
            return false;
        }

        std::uint32_t newst = 0x8000'0000;
        return state_.compare_exchange_strong( st, newst, std::memory_order_acquire, std::memory_order_relaxed );
    }

    void lock() noexcept
    {
        for( unsigned k = 0;; ++k )
        {
            std::uint32_t st = state_.load( std::memory_order_relaxed );

            if( st & 0x8000'0000 )
            {
                // locked exclusive, spin
            }
            else if( ( st & 0x3FFF'FFFF ) == 0 )
            {
                // not locked exclusive, not locked shared, try to lock

                std::uint32_t newst = 0x8000'0000;
                if( state_.compare_exchange_strong( st, newst, std::memory_order_acquire, std::memory_order_relaxed ) ) break;
            }
            else if( st & 0x4000'000 )
            {
                // writer pending bit already set, nothing to do
            }
            else
            {
                // locked shared, set writer pending bit

                std::uint32_t newst = st | 0x4000'0000;
                state_.compare_exchange_strong( st, newst, std::memory_order_relaxed, std::memory_order_relaxed );
            }

            yield_k( k );
        }
    }

    void unlock() noexcept
    {
        // pre: locked exclusive, not locked shared
        state_.store( 0, std::memory_order_release );
    }
};

#endif // RW_SPINLOCK_HPP_INCLUDED
