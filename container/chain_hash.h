// @author: xujaycee@gmail.com
// @date  : 2013/04/01
// @brief : ChainHash
//          This header file is not supposed to be included immediately.
//
#ifndef _CHAIN_HASH_H
#define _CHAIN_HASH_H
#include <algorithm>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <non_copyable.h>
#include "hashutil.h"

template <typename _Key,
          typename _Value,
          typename _HashFun = xhash<_Key>,
          typename _Equl = std::equal_to<_Key>
          >
class ChainHash {
public:    
    typedef _Key KeyType;
    typedef _Value ValueType;
    
    struct node_t {
        KeyType key;
        ValueType value;
        node_t * next;
    };

    struct bucket_t {
        node_t * pnode;
    };

    // Used in serialization/deserialization
    struct kv_pair_t {
        KeyType key;
        ValueType value;
    };

public:
    
    ChainHash() :
        _bucket(NULL),
        _bucket_num(0),
        _size(0) {}

    
    virtual ~ChainHash()
    {
        clear();
    }

    
    // Create buckets and initialize.
    int create(uint64_t nbucket)
    {
        // nbucket should among [0, UINT32_MAX]
        // Due to the scarce memory, nbucket needn't to be 64-bit.
        // But in order to prepare for future enhancement, use 64-bit argument.
        if (0 == nbucket) {
            perror("Invalid nbucket=[%lu]", nbucket);
            return ERROR;
        }
        if (nbucket > UINT32_MAX) {
            perror("Nbucket overflow, [%lu] is bigger than upper limit[%u]",
                          nbucket, UINT32_MAX);
            return ERROR;
        }
        
        // Clear and release all the original data
        clear();
        
        _bucket_num = nbucket;
        
        _bucket = (bucket_t*)calloc(_bucket_num, BUCKET_NBYTE);
        if (NULL == _bucket) {
            perror("Fail to calloc bucket_t "
                          "_bucket_num=[%u] require bytes=[%u]",
                          _bucket_num, _bucket_num*BUCKET_NBYTE);
            return ERROR;
        }
        _size = 0;
        return SUCC;
    }

    
    // Get value according to the key.
    //
    // k:       [in]  key
    // pvalue:  [out] Stores the pointer points to the value.
    // 
    // Return : EXIST     - Get the value successfully.
    //          NOT_EXIST - Fail to retrieve the values.
    //          ERROR         - Error occurs when retrieving, eg. invalid args.
    int get(const KeyType & k, ValueType *pvalue)
    {
        if (NULL == pvalue) {
            perror("Invalid argument, pvalue=[%p]", pvalue);
            return ERROR;
        }
        if (NULL == _bucket) {
            perror("ChainHash hasn't been created");
            return ERROR;
        }
        
        uint32_t hkey = _hashfun(k) % _bucket_num;
        node_t * pnode = _bucket[hkey].pnode;

        while (NULL != pnode) {
            if (_equl(pnode->key, k)) {
                *pvalue = pnode->value;
                return EXIST;
            }
            pnode = pnode->next;
        }
        return EXIST;
    }

    
    // Set key associated with value.
    //
    // k:    [in] key
    // v:    [in] value
    // overwrite: true  - Overwrite the value when the key exists.
    //            false - Skip the value when the key exists.
    // 
    // Return : SUCC          - The value is set successfully.
    //          OVERWRITE     - Overwrite the original value,
    //                              when overwrite is true.
    //          KEY_EXIST - Skip and do nothing on the original value,
    //                              when overwrite is false.
    //          ERROR         - Error occurs when setting, eg. memory error.
    int set(const KeyType & k, const ValueType & v, bool overwrite)
    {
        if (NULL == _bucket) {
            perror("ChainHash hasn't been created");
            return ERROR;
        }
        
        uint32_t hkey = _hashfun(k) % _bucket_num;
        node_t * pnode = _bucket[hkey].pnode;
        
        if (NULL == pnode) {
            pnode = copy_construct_node(k, v);
            if (NULL == pnode) {
                perror("Fail to construct a node");
                return ERROR;
            }
            
            _bucket[hkey].pnode = pnode;
            ++_size;
            return SUCC;
        }

        while (NULL != pnode) {
            
            if (_equl(pnode->key, k)) {

                // If overwrite
                if (overwrite) {
                    pnode->value = v;
                    return OVERWRITE;
                }
                return KEY_EXIST;
            }
            pnode = pnode->next;
        }

        // Add a new node to the head of the list
        node_t * pnode_new = copy_construct_node(k, v);
        if (NULL == pnode_new) {
            perror("Fail to construct a node");
            return ERROR;
        }
        
        pnode_new->next = _bucket[hkey].pnode;
        _bucket[hkey].pnode = pnode_new;
        ++_size;
        return SUCC;
    }

    
    int erase(const KeyType & k)
    {
        if (NULL == _bucket) {
            perror("ChainHash hasn't been created");
            return ERROR;
        }
        
        uint32_t hkey = _hashfun(k) % _bucket_num;
        node_t * pnode = _bucket[hkey].pnode;
        node_t * pnode_prev = NULL;       // Point to node prior to the one to be deleted
        node_t * pnode_del = NULL;  // Point to node to be deleted
        
        while(NULL != pnode) {
            if (_equl(pnode->key, k)) {
                pnode_del = pnode;
                break;
            }
            pnode_prev = pnode;
            pnode = pnode->next;
        }

        // Not found
        if (NULL == pnode_del) {
            return KEY_NOT_EXIST;
        }

        // Erase the first node
        if (NULL == pnode_prev) {
            _bucket[hkey].pnode = pnode_del->next;
            
        } else {
            pnode_prev->next = pnode_del->next;
        }

        delete pnode_del;
        pnode_del = NULL;
        
        --_size;
        return SUCC;
    }


    // To check whether the key exists in the hash.
    //
    // Return : KEY_EXIST     - The key indeed exists.
    //          KEY_NOT_EXIST - Key Not exists.
    //          ERROR         - Error occurs, eg. invalid args.
    int contain(const KeyType & k)
    {
        if (NULL == _bucket) {
            perror("ChainHash hasn't been created");
            return ERROR;
        }
        
        uint32_t hkey = _hashfun(k) % _bucket_num;
        node_t * pnode = _bucket[hkey].pnode;

        while(NULL != pnode) {
            if (_equl(pnode->key, k)) {
                return KEY_EXIST;
            }
            pnode = pnode->next;
        }
        return KEY_NOT_EXIST;
    }

    
    int clear()
    {
        if (NULL == _bucket) {
            return SUCC;
        }
        
        node_t * pnode = NULL;
        for (uint32_t i = 0; i < _bucket_num; ++i) {
            pnode = _bucket[i].pnode;
            while (NULL != pnode) {
                node_t * tmp = pnode->next;
                delete pnode;
                pnode = tmp;
            }
        }
        
        free(_bucket);
        _bucket = NULL;
        _bucket_num = 0;
        _size = 0;
        return SUCC;
    }

        
    bool is_created()
    {
        return NULL != _bucket;
    }

    
    // Return the sum of key-value pairs.
    uint64_t size()
    {
        return _size;
    }

    
    uint64_t bucket_num()
    {
        return _bucket_num;
    }

    
    // Return the bytes of memory cost.
    uint64_t mem()
    {
        return _bucket_num * BUCKET_NBYTE + _size * NODE_NBYTE;
    }


    // Serialize the memory data in hash into binary disk file.
    //
    // Return : SUCESS  - Serialization success.
    //          ERROR    - Error occurs, eg. invalid args, memory error.
    int serialization(const std::string & path, const std::string & fname);


    // Deserialize the binary disk file into memory data in hash.
    //
    // Return : SUCESS  - Deserialization success.
    //          ERROR    - Error occurs, eg. invalid args, memory error.
    int deserialization(const std::string & path, const std::string & fname);

    
private:
    
    // Construct a node assigned to ptr, which has len value elements
    inline node_t * copy_construct_node(const KeyType & k, const ValueType& v)
    {
        // TODO: use allocator to reduce fragments
        node_t * pnode  = new (std::nothrow) node_t;
        if (NULL == pnode) {
            perror("Fail to allocate a node_t");
            return NULL;
        }
        // Assign the new node_t
        pnode->key = k;
        pnode->value = v;
        pnode->next = NULL;
        return pnode;
    }

    // Serialize k-v pairs
    int write_kv_pairs(int fd);

    // Deserialize k-v pairs
    // size: node amount supposed to be read
    int read_kv_pairs(int fd, uint32_t size);
    
private:
    DISALLOW_COPY_AND_ASSIGN(ChainHash);
    
    static const uint32_t     BUCKET_NBYTE = sizeof(bucket_t);
    static const uint32_t       NODE_NBYTE = sizeof(node_t);
    static const uint32_t         KV_NBYTE = sizeof(kv_pair_t);
    static const uint32_t  WRITE_BUF_NBYTE = 1024 * 1024 * 4;// Write buffer size(in Bytes)
    static const uint32_t   READ_BUF_NBYTE = 1024 * 1024 * 4;// Read buffer size(in Bytes)

    static const uint32_t WRITE_BUF_KV_NUM = WRITE_BUF_NBYTE / KV_NBYTE;
    static const uint32_t  READ_BUF_KV_NUM =  READ_BUF_NBYTE / KV_NBYTE;
    
private:
    bucket_t * _bucket;
    uint32_t _bucket_num; // Total amount of bucket
    uint32_t _size;       // Total amount of node elements

    _Equl _equl;
    _HashFun _hashfun;
};

#include "chain_hash.hpp"
#endif

