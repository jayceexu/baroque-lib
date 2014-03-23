// Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved
// @author: xujian02@baidu.com
// @date  : 2013/05/09
// @brief : Implementation of serialization/deserialization methods of ChainHash
//
#ifndef UAP_UTILITY_DICTIONARY_CHAIN_HASH_HPP
#define UAP_UTILITY_DICTIONARY_CHAIN_HASH_HPP

namespace uap {
namespace utility {
namespace dictionary {

template <typename _Key, typename _Value, typename _HashFun, typename _Equl>
int ChainHash<_Key, _Value, _HashFun, _Equl>::serialization(const std::string & path,
                                                            const std::string & fname)
{
    char fullpath[UAP_MAX_PATH_LENGTH];
    fullpath[0] = '\0';
    int ret = ul_fullpath(path.c_str(), fname.c_str(),
                          fullpath, UAP_MAX_PATH_LENGTH);
    if (0 != ret) {
        UAP_LOG_FATAL("Fail to combine path=[%s] fname=[%s]",
                        path.c_str(), fname.c_str());
        return UAP_ERROR;
    }

    // Open file for serialization
    int fd = open(fullpath, O_WRONLY | O_CREAT | O_TRUNC);
    if (fd < 0) {
        UAP_LOG_FATAL("Fail to open file=[%s] error_msg=[%m]",
                        fullpath);
        return UAP_ERROR;
    }
    if (-1 == fchmod(fd, 0644)) {
        UAP_LOG_FATAL("Fail to change mode of file=[%s] error_msg=[%m]",
                        fullpath);
        return UAP_ERROR;
    }

    do {
        // Serialize _bucket_num, _size 
        ret = write(fd, &_bucket_num, sizeof(uint32_t));
        if (-1 == ret) {
            UAP_LOG_FATAL("Fail to serialize _bucket_num=[%u] error_msg=[%m]",
                            _bucket_num);
            break;
        }
        ret = write(fd, &_size, sizeof(uint32_t));
        if (-1 == ret) {
            UAP_LOG_FATAL("Fail to serialize _size=[%u] error_msg=[%m]",
                          _size);
            break;
        }
        
        // Serialize the node_t
        if (UAP_SUCC != write_kv_pairs(fd)) {
            UAP_LOG_FATAL("Fail to serialize node_t");
            break;
        }

        close(fd);
        UAP_LOG_TRACE("Serialize to file=[%s] succeed", fullpath);
        return UAP_SUCC;
            
    } while(false);

    close(fd);
    return UAP_ERROR;
}


template <typename _Key, typename _Value, typename _HashFun, typename _Equl>
int ChainHash<_Key, _Value, _HashFun, _Equl>::write_kv_pairs(int fd)
{
    kv_pair_t * output_buf = new (std::nothrow) kv_pair_t[WRITE_BUF_KV_NUM];
    if (NULL == output_buf) {
        UAP_LOG_FATAL("Fail to new output buffer err_msg=[%m]");
        return UAP_ERROR;
    }

    uint32_t * bucket_buf = (uint32_t*)calloc(_bucket_num, sizeof(uint32_t));
    if (NULL == bucket_buf) {
        UAP_LOG_FATAL("Fail to new bucket buffer err_msg=[%m]");
        delete output_buf;
        output_buf = NULL;
        return UAP_ERROR;
    }

    uint16_t * size_buf = (uint16_t*)calloc(_bucket_num, sizeof(uint16_t));
    if (NULL == size_buf) {
        UAP_LOG_FATAL("Fail to new size buffer err_msg=[%m]");
        delete output_buf;
        output_buf = NULL;
        free(size_buf);
        size_buf = NULL;
        return UAP_ERROR;
    }
    
    uint32_t counter = 0;  // serialize counter
    uint32_t cur_offset = 0;  // indicate traverse size currently
    uint32_t cur_size = 0; // indicate current list size
    
    // Serialize WRITE_BUF_KV_NUM nodes each time
    for (uint32_t i = 0; i < _bucket_num; ++i) {
        
        bucket_buf[i] = cur_offset;
        cur_size = 0;
        if (NULL == _bucket[i].pnode) {
            continue;
        }
        node_t * pnode = _bucket[i].pnode;
        do {
            ++cur_offset;
            ++cur_size;
            output_buf[counter].key = pnode->key;
            output_buf[counter].value = pnode->value;
            ++counter;
            // flush and output the buffer
            if (counter == WRITE_BUF_KV_NUM) {
                counter = 0;
                if (-1 == write(fd, output_buf,  WRITE_BUF_KV_NUM * KV_NBYTE)) {
                    UAP_LOG_FATAL("Fail to serialize node_unit err_msg=[%m]");
                    goto WRITE_FAIL;
                }
            }
            pnode = pnode->next;
            
        } while (NULL != pnode);
        size_buf[i] = cur_size;
    }

    // Serialize the remanent nodes
    if (counter > 0) {
        if (-1 == write(fd, output_buf, counter * KV_NBYTE)) {
            UAP_LOG_FATAL("Fail to serialize node_unit err_msg=[%m]");
            goto WRITE_FAIL;
        }
    }

    if (-1 == write(fd, bucket_buf, _bucket_num * sizeof(uint32_t))) {
        UAP_LOG_FATAL("Fail to serialize bucket_buf err_msg=[%m]");
        goto WRITE_FAIL;
    }
    if (-1 == write(fd, size_buf, _bucket_num * sizeof(uint16_t))) {
        UAP_LOG_FATAL("Fail to serialize size_buf err_msg=[%m]");
        goto WRITE_FAIL;
    }
    delete [] output_buf;
    output_buf = NULL;
    free(bucket_buf);
    bucket_buf = NULL;
    free(size_buf);
    size_buf = NULL;
    return UAP_SUCC;
    
WRITE_FAIL:
    delete [] output_buf;
    output_buf = NULL;
    free(bucket_buf);
    bucket_buf = NULL;
    free(size_buf);
    size_buf = NULL;
    return UAP_ERROR;
}


template <typename _Key, typename _Value, typename _HashFun, typename _Equl>
int ChainHash<_Key, _Value, _HashFun, _Equl>::deserialization(const std::string & path,
                                                              const std::string & fname)
{
    char fullpath[UAP_MAX_PATH_LENGTH];
    fullpath[0] = '\0';
    int ret = ul_fullpath(path.c_str(), fname.c_str(),
                          fullpath, UAP_MAX_PATH_LENGTH);
    if (0 != ret) {
        UAP_LOG_FATAL("Fail to combine path=[%s] fname=[%s]",
                        path.c_str(), fname.c_str());
        return UAP_ERROR;
    }
    int fd = open(fullpath, O_RDONLY);
    if (fd < 0) {
        UAP_LOG_FATAL("Fail to open file=[%s] err_msg=[%m]",
                        fullpath);
        return UAP_ERROR;
    }

    // Clear up all the original data
    clear();

    do {
        // Deserialize _bucket_num
        // TODO:read index by block
        ret = read(fd, &_bucket_num, sizeof(uint32_t));
        if (-1 == ret) {
            UAP_LOG_FATAL("Fail to deserialize bucket num err_msg=[%m]");
            break;
        }
        // Create buckets
        if (UAP_SUCC != create(_bucket_num)) {
            UAP_LOG_FATAL("Fail to create ChainHash");
            break;
        }
        
        uint32_t size = 0;
        ret = read(fd, &size, sizeof(uint32_t));
        if (-1 == ret) {
            UAP_LOG_FATAL("Fail to deserialize size err_msg=[%m]");
            break;
        }

        // Deserialize the node_t
        ret = read_kv_pairs(fd, size);
        if (UAP_SUCC != ret) {
            UAP_LOG_FATAL("Fail to deserialize node_t");
            break;
        }

        // Verify correctness
        if (_size != size) {
            UAP_LOG_FATAL("Verification failed. "
                            "Read size=[%u] doesn't meet supposed size=[%u]",
                            _size, size);
            break;
        }
        
        close(fd);
        return UAP_SUCC;
            
    } while(false);

    close(fd);
    return UAP_ERROR;
}

template <typename _Key, typename _Value, typename _HashFun, typename _Equl>
int ChainHash<_Key, _Value, _HashFun, _Equl>::read_kv_pairs(int fd, uint32_t size)
{
    
    // Allocate a buffer for read, to reduce the frequence of read procedure
    kv_pair_t * input_buf = new (std::nothrow) kv_pair_t[READ_BUF_KV_NUM];
    if (NULL == input_buf) {
        UAP_LOG_FATAL("Fail to new input buffer");
        return UAP_ERROR;
    }

    ssize_t nbyte = 0;
        
    while ((nbyte = read(fd, input_buf, READ_BUF_KV_NUM * KV_NBYTE)) > 0) {
            
        size_t n = nbyte / KV_NBYTE;
        for (size_t k = 0; k < n; ++k) {

            // If has read enough nodes
            if (0 == size) {
                break;
            }
            --size;
            int ret = set(input_buf[k].key, input_buf[k].value, true);
            if (UAP_ERROR == ret) {
                UAP_LOG_FATAL("Fail to set node_unit");
                continue;
            }
        }
    }
        
    delete [] input_buf;
    input_buf = NULL;
    return UAP_SUCC;
}


} //end of namespace dictionary
} //end of namespace utility
} //end of namespace uap

#endif

