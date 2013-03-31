

template <typename _Key,
          typename _Value,
          typename _Alloc,
          typename _Hash,
          typename _Equl>
class ArrayHash
{
    typedef _Key key_type;
    typedef _Value value_type;
    typedef _Hash hash_fun;
    typedef _Equl equl;
public:
    ArrayHash() {}
    virtual ~ArrayHash() {}
    
};
