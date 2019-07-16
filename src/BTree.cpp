//
// Created by Hao Wu on 7/12/19.
//
#include <string>

class BNode {
public:
    enum class Type {
        BT_INNER,
        BT_LEAF,
    };
    BNode(void *raw);
    BNode(const BNode&) = delete;
    BNode &operator = (const &BNode) = delete;
    virtual ~BNode();

    static BNode &&build(void *raw);
    Type type() const;
    void *data();

private:
    void *raw_data;
};
class BInnerNode : public BNode {
public:
    BInnerNode(void *raw);
    virtual ~BInnerNode();
};
class BLeafNode : public BNode {
public:
    BLeafNode(void *raw);
    virtual ~BLeafNode();

};

class BTree {
public:
    BTree(const std::string &db_file);

private:
    std::string db_file;
};

BNode&& BNode::build(void *raw) {
    BNode::Type *tp = raw;
    if (*tp == BNode::Type::BT_INNER)
        return BInnerNode(raw);
    if (*tp == BNode::Type::BT_LEAF)
        return BLeafNode(raw);
    return BNode(nullptr);
}