#include <bits/stdc++.h>
using namespace std::chrono;
using namespace std;

template <typename T>
class BPlusTree {
private:
    struct Node {
        vector<T> keys;
        vector<shared_ptr<Node>> children;
        shared_ptr<Node> next;
        bool isLeaf;
        
        Node(bool leaf = false) : isLeaf(leaf), next(nullptr) {}
    };
    
    shared_ptr<Node> root;
    int degree;
    
    void splitChild(shared_ptr<Node> parent, int index) {
        shared_ptr<Node> fullChild = parent->children[index];
        shared_ptr<Node> newChild = make_shared<Node>(fullChild->isLeaf);
        
        int mid = degree - 1;
        
        newChild->keys.assign(fullChild->keys.begin() + mid + 1, fullChild->keys.end());
        fullChild->keys.resize(mid);
        
        if (!fullChild->isLeaf) {
            newChild->children.assign(fullChild->children.begin() + mid + 1, fullChild->children.end());
            fullChild->children.resize(mid + 1);
        }
        
        else {
            newChild->next = fullChild->next;
            fullChild->next = newChild;

            newChild->keys.insert(newChild->keys.begin(), fullChild->keys[mid]);
        }
        
        parent->children.insert(parent->children.begin() + index + 1, newChild);
        parent->keys.insert(parent->keys.begin() + index, fullChild->keys[mid]);
    }
    
    void insertNonFull(shared_ptr<Node> node, const T& key) {
        int i = node->keys.size() - 1;
        
        if (node->isLeaf) {
            node->keys.push_back(T{});
            while (i >= 0 && key < node->keys[i]) {
                node->keys[i + 1] = node->keys[i];
                i--;
            }
            node->keys[i + 1] = key;
        } 
        
        else {
            while (i >= 0 && key < node->keys[i]) {
                i--;
            }
            i++;
            
            if (node->children[i]->keys.size() == 2 * degree - 1) {
                splitChild(node, i);
                if (key > node->keys[i]) {
                    i++;
                }
            }
            insertNonFull(node->children[i], key);
        }
    }
    
    T getPredecessor(shared_ptr<Node> node, int idx) {
        shared_ptr<Node> curr = node->children[idx];
        while (!curr->isLeaf) {
            curr = curr->children[curr->keys.size()];
        }
        return curr->keys[curr->keys.size() - 1];
    }
    
    T getSuccessor(shared_ptr<Node> node, int idx) {
        shared_ptr<Node> curr = node->children[idx + 1];
        while (!curr->isLeaf) {
            curr = curr->children[0];
        }
        return curr->keys[0];
    }
    
    void fill(shared_ptr<Node> node, int idx) {
        if (idx != 0 && node->children[idx - 1]->keys.size() >= degree) {
            borrowFromPrev(node, idx);
        } 
        
        else if (idx != node->keys.size() && node->children[idx + 1]->keys.size() >= degree) {
            borrowFromNext(node, idx);
        }
        
        else {
            if (idx != node->keys.size()) {
                merge(node, idx);
            } 
            
            else {
                merge(node, idx - 1);
            }
        }
    }
    
    void borrowFromPrev(shared_ptr<Node> node, int idx) {
        shared_ptr<Node> child = node->children[idx];
        shared_ptr<Node> sibling = node->children[idx - 1];
        
        if (child->isLeaf) {
            child->keys.insert(child->keys.begin(), sibling->keys.back());
            node->keys[idx - 1] = sibling->keys.back();
            sibling->keys.pop_back();
        }
        
        else {
            child->keys.insert(child->keys.begin(), node->keys[idx - 1]);
            node->keys[idx - 1] = sibling->keys.back();
            sibling->keys.pop_back();
            
            child->children.insert(child->children.begin(), sibling->children.back());
            sibling->children.pop_back();
        }
    }
    
    void borrowFromNext(shared_ptr<Node> node, int idx) {
        shared_ptr<Node> child = node->children[idx];
        shared_ptr<Node> sibling = node->children[idx + 1];
        
        if (child->isLeaf) {
            child->keys.push_back(sibling->keys[0]);
            node->keys[idx] = sibling->keys[1];
            sibling->keys.erase(sibling->keys.begin());
        } 
        
        else {
            child->keys.push_back(node->keys[idx]);
            node->keys[idx] = sibling->keys[0];
            sibling->keys.erase(sibling->keys.begin());
            
            child->children.push_back(sibling->children[0]);
            sibling->children.erase(sibling->children.begin());
        }
    }
    
    void merge(shared_ptr<Node> node, int idx) {
        shared_ptr<Node> child = node->children[idx];
        shared_ptr<Node> sibling = node->children[idx + 1];
        
        if (!child->isLeaf) {
            child->keys.push_back(node->keys[idx]);
        }
        
        child->keys.insert(child->keys.end(), sibling->keys.begin(), sibling->keys.end());
        
        if (!child->isLeaf) {
            child->children.insert(child->children.end(), sibling->children.begin(), sibling->children.end());
        }
        
        else {
            child->next = sibling->next;
        }
        
        node->keys.erase(node->keys.begin() + idx);
        node->children.erase(node->children.begin() + idx + 1);
    }
    
    void removeFromLeaf(shared_ptr<Node> node, int idx) {
        node->keys.erase(node->keys.begin() + idx);
    }
    
    void removeFromNonLeaf(shared_ptr<Node> node, int idx) {
        T key = node->keys[idx];
        
        if (node->children[idx]->keys.size() >= degree) {
            T pred = getPredecessor(node, idx);
            node->keys[idx] = pred;
            removeHelper(node->children[idx], pred);
        } 
        
        else if (node->children[idx + 1]->keys.size() >= degree) {
            T succ = getSuccessor(node, idx);
            node->keys[idx] = succ;
            removeHelper(node->children[idx + 1], succ);
        } 
        
        else {
            merge(node, idx);
            removeHelper(node->children[idx], key);
        }
    }
    
    void removeHelper(shared_ptr<Node> node, const T& key) {
        int idx = lower_bound(node->keys.begin(), node->keys.end(), key) - node->keys.begin();
        
        if (idx < node->keys.size() && node->keys[idx] == key) {
            if (node->isLeaf) {
                removeFromLeaf(node, idx);
            } 
            
            else {
                removeFromNonLeaf(node, idx);
            }
        } else if (!node->isLeaf) {
            bool isLastChild = (idx == node->keys.size());
            
            if (node->children[idx]->keys.size() < degree) {
                fill(node, idx);
            }
            
            if (isLastChild && idx > node->keys.size()) {
                removeHelper(node->children[idx - 1], key);
            } else {
                removeHelper(node->children[idx], key);
            }
        }
    }
    
    shared_ptr<Node> searchHelper(shared_ptr<Node> node, const T& key) {
        if (!node) return nullptr;
        
        int i = 0;
        while (i < node->keys.size() && key > node->keys[i]) {
            i++;
        }
        
        if (i < node->keys.size() && key == node->keys[i] && node->isLeaf) {
            return node;
        }
        
        if (node->isLeaf) {
            return nullptr;
        }
        
        return searchHelper(node->children[i], key);
    }
    
    void printHelper(shared_ptr<Node> node, int level = 0) {
        if (!node) return;
        
        cout << "Level " << level << ": ";
        for (const T& key : node->keys) {
            cout << key << " ";
        }
        cout << (node->isLeaf ? "(leaf)" : "(internal)") << endl;
        
        if (!node->isLeaf) {
            for (auto& child : node->children) {
                printHelper(child, level + 1);
            }
        }
    }

    int countNodes(shared_ptr<Node> node) const {
        if (!node) return 0;
        int count = 1;
        if (!node->isLeaf) {
            for (auto& child : node->children)
                count += countNodes(child);
        }
        return count;
    }

    size_t estimateMemoryUsageHelper(const shared_ptr<Node>& node) const {
    if (!node) return 0;

    size_t mem = sizeof(Node);

    mem += node->keys.capacity() * sizeof(T);
    mem += node->children.capacity() * sizeof(shared_ptr<Node>);

    for (const auto& child : node->children) {
        mem += estimateMemoryUsageHelper(child);
    }

    return mem;
}


public:
    explicit BPlusTree(int minDegree = 3) : degree(minDegree), root(nullptr) {}

    void insert(const T& key) {
        if (!root) {
            root = make_shared<Node>(true);
            root->keys.push_back(key);
        } else {
            if (root->keys.size() == 2 * degree - 1) {
                auto newRoot = make_shared<Node>(false);
                newRoot->children.push_back(root);
                splitChild(newRoot, 0);
                root = newRoot;
            }
            insertNonFull(root, key);
        }
    }

    bool find(const T& key) {
        return searchHelper(root, key) != nullptr;
    }

    bool remove(const T& key) {
        if (!root) return false;
        removeHelper(root, key);
        if (root->keys.empty()) {
            if (!root->isLeaf)
                root = root->children[0];
            else
                root = nullptr;
        }
        return true;
    }

    bool update(const T& oldKey, const T& newKey) {
        if (remove(oldKey)) {
            insert(newKey);
            return true;
        }
        return false;
    }

size_t estimateMemoryUsage() const {
    return estimateMemoryUsageHelper(root);
}

};


int main() {
    vector<int> sizes = {100, 500, 1000};
    for (int N : sizes) {
        cout << "\n========== N = " << N << " ==========\n";
        vector<int> data(N);
        mt19937 rng(42);
        uniform_int_distribution<int> dist(1, 1e6);
        for (int i = 0; i < N; ++i)
            data[i] = dist(rng);

        BPlusTree<int> bptree(3);
        unordered_map<int, bool> hashmap;

        // CREATE / INSERT
        auto start = high_resolution_clock::now();
        for (int x : data) bptree.insert(x);
        auto end = high_resolution_clock::now();
        cout << "B+ Tree   INSERT : " << duration_cast<milliseconds>(end - start).count() << " ms\n";

        start = high_resolution_clock::now();
        for (int x : data) hashmap[x] = true;
        end = high_resolution_clock::now();
        cout << "Hash Map  INSERT : " << duration_cast<milliseconds>(end - start).count() << " ms\n";

        // READ / FIND
        start = high_resolution_clock::now();
        for (int x : data) bptree.find(x);
        end = high_resolution_clock::now();
        cout << "B+ Tree   FIND   : " << duration_cast<milliseconds>(end - start).count() << " ms\n";

        start = high_resolution_clock::now();
        for (int x : data) hashmap.count(x);
        end = high_resolution_clock::now();
        cout << "Hash Map  FIND   : " << duration_cast<milliseconds>(end - start).count() << " ms\n";

        // UPDATE
        start = high_resolution_clock::now();
        for (int i = 0; i < N; ++i) {
            bptree.remove(data[i]);
            data[i] += 1;
            bptree.insert(data[i]);
        }
        end = high_resolution_clock::now();
        cout << "B+ Tree   UPDATE : " << duration_cast<milliseconds>(end - start).count() << " ms\n";

        start = high_resolution_clock::now();
        unordered_map<int, bool> newmap;
        for (int i = 0; i < N; ++i) {
            hashmap.erase(data[i] - 1);
            newmap[data[i]] = true;
        }
        hashmap = std::move(newmap);
        end = high_resolution_clock::now();
        cout << "Hash Map  UPDATE : " << duration_cast<milliseconds>(end - start).count() << " ms\n";

        size_t bptreeMem = bptree.estimateMemoryUsage();

        size_t entrySize = sizeof(pair<const int, bool>) + sizeof(void*);
        size_t hashmapMem = hashmap.size() * entrySize;
        size_t bucketOverhead = hashmap.bucket_count() * sizeof(void*);
        hashmapMem += bucketOverhead;

        // DELETE
        start = high_resolution_clock::now();
        for (int x : data) bptree.remove(x);
        end = high_resolution_clock::now();
        cout << "B+ Tree   DELETE : " << duration_cast<milliseconds>(end - start).count() << " ms\n";

        start = high_resolution_clock::now();
        for (int x : data) hashmap.erase(x);
        end = high_resolution_clock::now();
        cout << "Hash Map  DELETE : " << duration_cast<milliseconds>(end - start).count() << " ms\n";

        cout << "B+ Tree   Memory Allocation: " << bptreeMem << " bytes\n";
        cout << "Hash Map  Memory Allocation: " << hashmapMem << " bytes\n";
    }
    return 0;
}


