#include <pthread.h> //> pthread_spinlock_t
#include <limits.h>

#include "alloc.h"

#define CACHE_LINE_SIZE 64
#define MINVAL -999999

typedef struct bst_node {
	int key;
	int valid; 			//> Valid = 1 => node exists, otherwise valid = 0
	struct bst_node *pred;
	struct bst_node *succ;
	struct bst_node *parent;
	struct bst_node *link[2];
	void *value;

	pthread_spinlock_t succLock;
	pthread_spinlock_t treeLock;

	// FILL the padding
	char padding[CACHE_LINE_SIZE - 2 * sizeof(int) -
	             5 * sizeof(struct bst_node *) - sizeof(void *) - 2 * sizeof(pthread_spinlock_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) bst_node_t;

typedef struct {
	bst_node_t *root;

} bst_t;

static bst_node_t *bst_node_new(int key, void *value, bst_node_t *pred, bst_node_t *succ, bst_node_t *parent)
{
        bst_node_t *ret;

        XMALLOC(ret, 1);
        ret->key = key;
	ret->valid = 1;
	ret->pred = pred;
	ret->succ = succ;
	ret->parent = parent;
	ret->link[0] = NULL;
	ret->link[1] = NULL;
        ret->value = value;

	pthread_spin_init(&ret->succLock, PTHREAD_PROCESS_SHARED);
	pthread_spin_init(&ret->treeLock, PTHREAD_PROCESS_SHARED);

        return ret;
}

bst_t *_bst_new_helper()
{
	bst_t *bst;
	bst_node_t *parent;
	
	parent = bst_node_new(MINVAL, NULL, NULL, NULL, NULL);
	XMALLOC(bst, 1);
	bst->root = bst_node_new(INT_MAX, NULL, parent, parent, parent);
	bst->root->parent = parent;
	parent->link[1] = bst->root; 		//> Right child
	parent->succ = bst->root;
	parent->pred = bst->root;
	parent->parent = bst->root;
	parent->link[0] = bst->root;

	return bst;
}

static bst_node_t *lockParent(bst_node_t * node)
{	
	bst_node_t *parent = node->parent;		
	pthread_spin_lock(&parent->treeLock);

	while ((node->parent != parent) || (parent->valid == 0)) {
		pthread_spin_unlock(&parent->treeLock);
		parent = node->parent;
		while (parent->valid == 0) {
			parent = node->parent;
		}
		pthread_spin_lock(&parent->treeLock);
	}

	return parent;
}

static int _bst_lookup_helper(bst_t *bst, int key)
{ 
	int dir, currKey;
	bst_node_t *node, *child = NULL;	

	node = bst->root;
	while(1){
		currKey = node->key;
		if(currKey == key)
			break;
		dir = currKey < key;
		child = node->link[dir];
		if(child == NULL) 
			break;		
		node = child;
	}

	while(node->key > key)
		node = node->pred;

	while(node->key < key)
		node = node->succ;
	
	return ((node->key == key) && (node->valid == 1));
}

static int _bst_insert_helper(bst_t *bst, bst_node_t *new_node)
{ 
	int inserted = 0;
	bst_node_t *node = NULL;
	int key = new_node->key;

	while(1){ 
		//> Searh operation
		int dir, currKey;
		bst_node_t *node, *child = NULL;
		node = bst->root;
		while(1){
			currKey = node->key;
			if(currKey == key)
				break;
			dir = currKey < key;
			child = node->link[dir];
			if(child == NULL) 
				break;		
			node = child;
		}

		bst_node_t *p = (node->key >= key) ? node->pred : node;
		pthread_spin_lock(&p->succLock);
		bst_node_t *s = p->succ;  

		if((p->key < key) && (s->key >= key) && (p->valid == 1)){

			if(s->key == key){			//> The key already exists -  Unsuccessful insert 
				pthread_spin_unlock(&p->succLock);
				return inserted; 	
			}

			//> Find the right parent for new node - ChooseParent
			bst_node_t *parent = ((node ==  p) || (node == s)) ? node : p;
			while(1){
				pthread_spin_lock(&parent->treeLock);
				if(parent == p){
					if(parent->link[1] == NULL)
						break;
					pthread_spin_unlock(&parent->treeLock);
					parent = s;
				}else{
					if(parent->link[0] == NULL)
						break;
					pthread_spin_unlock(&parent->treeLock);
					parent = p;
				}
			}

			//> Update logical ordering layout
			new_node->succ = s;
			new_node->pred = p;
			new_node->parent = parent;		//> Parent is already locked
			s->pred = new_node;
			p->succ = new_node;
			pthread_spin_unlock(&p->succLock);
			
			//> Update physical layout - InsertToTree
								//> Parent is already locked
			if(parent->key < new_node->key){	//> New_node is the right child
				parent->link[1] = new_node;
			}else{					//> New_node is the left child
				parent->link[0] = new_node;
			}
			pthread_spin_unlock(&parent->treeLock);	//> Unlock parent's treeLock

			inserted = 1;
			return inserted;			//> Successful insert					
		}
		pthread_spin_unlock(&p->succLock);		//> Validation failed - restart
	}
	return inserted;
}

static int acquireTreeLocks(bst_node_t *node)
{
	while(1){
		pthread_spin_lock(&node->treeLock);
		bst_node_t *left = node->link[0];
		bst_node_t *right = node->link[1];

		if(left == NULL || right == NULL){		//> node is a leaf or has a single child
			if(left != NULL && pthread_spin_trylock(&left->treeLock) != 0){
				pthread_spin_unlock(&node->treeLock);
				continue;
			}
			if(right != NULL && pthread_spin_trylock(&right->treeLock) != 0){
				pthread_spin_unlock(&node->treeLock);
				continue;
			}
			return 0;				//> 0 => false
		}
		
		// n has two children
		bst_node_t *s = node->succ;
		bst_node_t *parent = s->parent;

		if(parent != node){		
			if(pthread_spin_trylock(&parent->treeLock) != 0){
				pthread_spin_unlock(&node->treeLock);
				continue;
			}
			if(parent != s->parent || parent->valid == 0){
				pthread_spin_unlock(&node->treeLock);
				pthread_spin_unlock(&parent->treeLock);
				continue;
			}
		}

		if(pthread_spin_trylock(&s->treeLock) != 0){
			pthread_spin_unlock(&node->treeLock);
			if(parent != node)		
				pthread_spin_unlock(&parent->treeLock);
			continue;
		}
		
		/*
		 * s has no left child
		 * s is the left most node in node's right subtree
		 * it may have right child
		 */
		bst_node_t *sRight = s->link[1];
		if(sRight != NULL && pthread_spin_trylock(&sRight->treeLock) != 0){
			pthread_spin_unlock(&node->treeLock);
			pthread_spin_unlock(&s->treeLock);
			if(parent != node)		
				pthread_spin_unlock(&parent->treeLock);
			continue;
		}
		return 1;				//> 1 => true (it has two children)
	}
}

static void removeFromTree(bst_node_t *node, int hasTwoChildren, bst_node_t *parent, bst_node_t *node_to_delete)
{
	if(hasTwoChildren == 0){			//> node is a leaf or has one single child
		bst_node_t *child = (node->link[1] == NULL) ? node->link[0] : node->link[1];

		//> UpdateChild
		if(child != NULL)
			child->parent = parent;
		int isLeft = 0;
		if(parent->link[0] == node)
			isLeft = 1;
		if(isLeft == 1){
			parent->link[0] = child;
		}else{
			parent->link[1] = child;
		}

		node_to_delete = node;
		pthread_spin_unlock(&parent->treeLock);
		pthread_spin_unlock(&node->treeLock);
		if(child != NULL)
			pthread_spin_unlock(&child->treeLock);
		return;
	}
		
	bst_node_t *succ = node->succ;
	bst_node_t *oldParent = succ->parent;
	bst_node_t *oldRight = succ->link[1];		//> oldRight may be NULL

	//> UpdateChild
	if(oldRight != NULL)
		oldRight->parent = oldParent;
	int left = 0;
	if(oldParent->link[0] == succ)
		left = 1;
	if(left == 1){
		oldParent->link[0] = oldRight;
	}else{
		oldParent->link[1] = oldRight;
	}

	succ->parent = parent;
	succ->link[0] = node->link[0];
	succ->link[1] = node->link[1];
	node->link[0]->parent = succ;
	if(node->link[1] != NULL)		//> n.right  may be null
		node->link[1]->parent = succ;
	if(parent->link[0] == node){
		parent->link[0] = succ;
	}else{
		parent->link[1] = succ;
	}

	pthread_spin_unlock(&succ->treeLock);
	pthread_spin_unlock(&node->treeLock);
	node_to_delete = node;
	pthread_spin_unlock(&parent->treeLock);

	if(oldParent != node)
		pthread_spin_unlock(&oldParent->treeLock);

	if(oldRight != NULL)
		pthread_spin_unlock(&oldRight->treeLock);

	return;
}

static inline int _bst_delete_helper(bst_t *bst, int key, bst_node_t *node_to_delete)
{
	int ret = 0;

	while(1){ 
		//> Searh operation
		int dir, currKey;
		bst_node_t *node, *child = NULL;
		node = bst->root;
		while(1){
			currKey = node->key;
			if(currKey == key)
				break;
			dir = currKey < key;
			child = node->link[dir];
			if(child == NULL) 
				break;		
			node = child;
		}

		bst_node_t *p = (node->key >= key) ? node->pred : node;
		pthread_spin_lock(&p->succLock);
		bst_node_t *s = p->succ;  

		if((p->key < key) && (s->key >= key) && (p->valid == 1)){

			if(s->key > key){			//> The key doesn't exist -  Unsuccessful delete
				pthread_spin_unlock(&p->succLock);
				return ret; 	
			}

			pthread_spin_lock(&s->succLock);	//> Successful remove
			int hasTwoChildren = acquireTreeLocks(s);
			bst_node_t *sParent = lockParent(s);

			//> Update logical order
			s->valid = 0;
			bst_node_t *sSucc = s->succ;
			sSucc->pred = p;
			p->succ = sSucc;
			pthread_spin_unlock(&s->succLock);
			pthread_spin_unlock(&p->succLock);
	
			//> Physical remove
			removeFromTree(s, hasTwoChildren, sParent, node_to_delete);
			ret = 1;
			return ret;		
		}
		pthread_spin_unlock(&p->succLock);		//> Validation failed - restart
	}
	return ret;
}

static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes;
static int bst_violations, logic_violations;
static void _bst_validate(bst_node_t *root, int _th)
{
	if (root == NULL)
		return;

	bst_node_t *left = root->link[0];
	bst_node_t *right = root->link[1];

	total_nodes++;
	_th++;

	/* BST violation? */
	if (left != NULL && left->key >= root->key)
		bst_violations++;
	if (right != NULL && right->key <= root->key)
		bst_violations++;

	/* Violation in logical order */
	if (root->pred->succ != root)
		logic_violations++;
	if (root->succ->pred != root)
		logic_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (left == NULL || right == NULL) {
		total_paths++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left != NULL){
		_bst_validate(left, _th);
	}
	if (right != NULL){
		_bst_validate(right, _th);
	}
}


static inline int _bst_validate_helper(bst_node_t *root)
{
	int check_bst = 0, check_logic = 0;
	int check_rbt = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;
	logic_violations = 0;

	_bst_validate(root, 0);

	check_bst = (bst_violations == 0);
	check_logic = (logic_violations == 0);
	check_rbt = (check_logic && check_bst);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  Valid Red-Black Tree: %s\n",
	       check_rbt ? "Yes [OK]" : "No [ERROR]");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Logical Violation: %s\n",
	       check_logic ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size (Total): %8d\n",
	       total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_bst;
}

static inline int _bst_warmup_helper(bst_t *bst, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	bst_node_t *node;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		node = bst_node_new(key, NULL, NULL, NULL, NULL);

		ret = _bst_insert_helper(bst, node); 
		nodes_inserted += ret;

		if (!ret) {
			free(node);
		}
	}

	return nodes_inserted;
}

/******************************************************************************/
/* BST Logical Ordering Search tree interface implementation                  */
/******************************************************************************/
void *bst_new()
{
	printf("Size of tree node is %lu\n", sizeof(bst_node_t));
	return _bst_new_helper();
}

int bst_lookup(void *bst, void *thread_data, int key)
{
	int ret;

	ret = _bst_lookup_helper(bst, key);

	return ret;
}

int bst_insert(void *bst, void *thread_data, int key, void *value)
{
	int ret;
	bst_node_t *node;

	node = bst_node_new(key, value, NULL, NULL, NULL);

	ret = _bst_insert_helper(bst, node);

	if (!ret) {
		free(node);
	}

	return ret;
}

int bst_delete(void *bst, void *thread_data, int key)
{
	int ret;
	bst_node_t *node_to_delete=NULL;

	ret = _bst_delete_helper(bst, key, node_to_delete);

	if (ret) {
		free(node_to_delete);
	}

	return ret;
}

int bst_validate(void *bst)
{
	int ret;
	ret = _bst_validate_helper(((bst_t *)bst)->root);
	return ret;
}

int bst_warmup(void *bst, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret;
	ret = _bst_warmup_helper((bst_t *)bst, nr_nodes, max_key, seed, force);
	return ret;
}

char *bst_name()
{
	return "bst_logical_ordering";
}




