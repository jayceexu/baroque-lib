// @author: xujaycee@gmail.com
// @date  : 2013/03/20
// @brief : Basic definition
//
#ifndef _HASHUTIL_H
#define _HASHUTIL_H
#include <string>

// Round x up to the nearest power of 2
static inline size_t roundup_power_of_two(size_t x)
{
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    x++;
    return x;
}

// Following xhash functions are cited from bsl library
template <class _Key>
struct xhash { 
    inline size_t operator () (const _Key & _k) const
    {
        return (size_t)_k;
    }
};

inline size_t
__hash_string(const char* __s)
{
    if (__s == 0) return 0;
    unsigned long __h = 0;
    for ( ; *__s; ++__s)
        __h = 5*__h + *__s;
    return size_t(__h);
}

template <> struct xhash<std::string>  {
    size_t operator () (const std::string & _k) const {
        return __hash_string(_k.c_str());
    }
};

template <> struct xhash<const std::string>  {
    size_t operator () (const std::string & _k) const {
        return __hash_string(_k.c_str());
    }
};

template<> struct xhash<char*>
{
    size_t operator()(const char* __s) const
    { return __hash_string(__s); }
};

template<> struct xhash<const char*>
{
    size_t operator()(const char* __s) const
    { return __hash_string(__s); }
};


#endif
