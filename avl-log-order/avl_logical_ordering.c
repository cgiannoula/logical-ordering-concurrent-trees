#include <pthread.h> //> pthread_spinlock_t
#include <limits.h>

#include "alloc.h"

#define CACHE_LINE_SIZE 64
#define MINVAL -999999
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define GET_BALANCE_FACTOR(node) ( node->leftHeight - node->rightHeight )

typedef struct avl_node {
	int key;
	int valid; 			//> Valid = 1 => node exists, otherwise valid = 0
	struct avl_node *pred;
	struct avl_node *succ;
	struct avl_node *parent;
	struct avl_node *link[2];
	int leftHeight;
	int rightHeight;
	void *value;

	pthread_spinlock_t succLock;
	pthread_spinlock_t treeLock;

	// FILL the padding
	char padding[2 * CACHE_LINE_SIZE - 4 * sizeof(int) -
	             5 * sizeof(struct avl_node *) - sizeof(void *) - 2 * sizeof(pthread_spinlock_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) avl_node_t;

typedef struct {
	avl_node_t *root;

} avl_t;

static avl_node_t *avl_node_new(int key, void *value, avl_node_t *pred, avl_node_t *succ, avl_node_t *parent)
{
        avl_node_t *ret;

        XMALLOC(ret, 1);
        ret->key = key;
	ret->valid = 1;
	ret->pred = pred;
	ret->succ = succ;
	ret->parent = parent;
	ret->link[0] = NULL;
	ret->link[1] = NULL;
	ret->leftHeight = 0;
	ret->rightHeight = 0;
        ret->value = value;

	pthread_spin_init(&ret->succLock, PTHREAD_PROCESS_SHARED);
	pthread_spin_init(&ret->treeLock, PTHREAD_PROCESS_SHARED);

        return ret;
}

avl_t *_avl_new_helper()
{
	avl_t *avl;
	avl_node_t *parent;
	
	parent = avl_node_new(MINVAL, NULL, NULL, NULL, NULL);
	XMALLOC(avl, 1);
	avl->root = avl_node_new(INT_MAX, NULL, parent, parent, parent);
	avl->root->parent = parent;
	parent->link[1] = avl->root; 		//> Right child
	parent->succ = avl->root;
	parent->pred = avl->root;
	parent->parent = avl->root;
	parent->link[0] = avl->root;

	return avl;
}

static avl_node_t *lockParent(avl_node_t * node)
{	
	avl_node_t *parent = node->parent;		
	pthread_spin_lock(&parent->treeLock);

	while ((node->parent != parent) || !parent->valid) {
		pthread_spin_unlock(&parent->treeLock);
		parent = node->parent;
		while(!parent->valid){
			parent = node->parent;
		}
		pthread_spin_lock(&parent->treeLock);
	}

	return parent;
}

static int acquireTreeLocks(avl_node_t *node)
{
	while(1){
		pthread_spin_lock(&node->treeLock);
		avl_node_t *left = node->link[0];
		avl_node_t *right = node->link[1];

		if(left == NULL || right == NULL){		//> node is a leaf or has a single child
			if(left != NULL && pthread_spin_trylock(&left->treeLock) != 0){	//> fail lock
				pthread_spin_unlock(&node->treeLock);
				continue;
			}
			if(right != NULL && pthread_spin_trylock(&right->treeLock) != 0){
				pthread_spin_unlock(&node->treeLock);
				continue;
			}
			return 0;				//> 0 => false (node hasn't two children)
		}
		
		// n has two children
		avl_node_t *s = node->succ;
		avl_node_t *parent = s->parent;

		if(parent != node){		
			if(pthread_spin_trylock(&parent->treeLock) != 0){
				pthread_spin_unlock(&node->treeLock);
				continue;
			}
			if(parent != s->parent || !parent->valid){
				pthread_spin_unlock(&parent->treeLock);
				pthread_spin_unlock(&node->treeLock);
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
		avl_node_t *sRight = s->link[1];
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

static int updateHeight(avl_node_t *ch, avl_node_t *node, int isLeft)
{
	int newHeight = ch == NULL? 0: MAX(ch->leftHeight, ch->rightHeight) + 1;
	int oldHeight = isLeft? node->leftHeight : node->rightHeight;
	if(newHeight == oldHeight) return 0;	
	if(isLeft)
		node->leftHeight = newHeight;
	else
		node->rightHeight = newHeight;
	
	return 1;
}

static int restart(avl_node_t *node, avl_node_t *parent)
{
	if(parent != NULL)
		pthread_spin_unlock(&parent->treeLock);

	while(1){ 
		pthread_spin_unlock(&node->treeLock);
		pthread_spin_lock(&node->treeLock);
		if(!node->valid){
			pthread_spin_unlock(&node->treeLock);
			return 0;
		}
		avl_node_t *child = GET_BALANCE_FACTOR(node) >= 2? node->link[0] : node->link[1];
		if(child == NULL) return 1;
		if(pthread_spin_trylock(&child->treeLock) == 0) return 1;	// success
	}
}

static void rotate(avl_node_t *child, avl_node_t *node, avl_node_t *parent, int left)
{
	if(parent->link[0] == node)
		parent->link[0] = child;
	else
		parent->link[1] = child;
	
	child->parent = parent;		
	node->parent = child;

	avl_node_t *grandChild = left? child->link[0] : child->link[1];
	if(left){
		node->link[1] = grandChild;
		if(grandChild != NULL){
			grandChild->parent = node; 
		}
		child->link[0] = node;
		node->rightHeight = child->leftHeight;
		child->leftHeight = MAX(node->leftHeight, node->rightHeight) + 1;
	}else{
		node->link[0] = grandChild;
		if(grandChild != NULL){
			grandChild->parent = node; 
		}
		child->link[1] = node;
		node->leftHeight = child->rightHeight;
		child->rightHeight = MAX(node->leftHeight, node->rightHeight) + 1;
	}
}

static void rebalance(avl_t *avl, avl_node_t *nod, avl_node_t *ch, int left)
{
	avl_node_t *node = nod;
	avl_node_t *child = ch;
	int isLeft = left;

	if(node == avl->root){
		pthread_spin_unlock(&node->treeLock);
		if(child != NULL) pthread_spin_unlock(&child->treeLock); 
			return;
	}

	avl_node_t *parent = NULL;
	while(node != avl->root){ 
		int updated = updateHeight(child, node, isLeft);
		int bf = GET_BALANCE_FACTOR(node);
		if(!updated && abs(bf) < 2) break;
		while(bf >= 2 || bf <= -2){ 
			if((isLeft && bf <= -2) || (!isLeft && bf >= 2)){ 
				if(child != NULL) pthread_spin_unlock(&child->treeLock); 
				child = isLeft? node->link[1] : node->link[0]; 
				if(pthread_spin_trylock(&child->treeLock) != 0){ 
					if(!restart(node, parent)){ 
						return;			
					}
					parent = NULL;
					bf = GET_BALANCE_FACTOR(node);
					child = bf >= 2? node->link[0] : node->link[1];
					isLeft = node->link[0] == child; 	
					continue;
				}
				isLeft = isLeft ^ 0x0001;		
			}
			
			if((isLeft && GET_BALANCE_FACTOR(child) < 0) || (!isLeft && GET_BALANCE_FACTOR(child) > 0)){ 
				avl_node_t *grandChild =  isLeft? child->link[1] : child->link[0]; 	
				if(pthread_spin_trylock(&grandChild->treeLock) != 0){		//> fail lock
					pthread_spin_unlock(&child->treeLock);
					if(!restart(node, parent)){ 
						return;			
					}
					parent = NULL;  
					bf = GET_BALANCE_FACTOR(node);
					child = bf >= 2? node->link[0] : node->link[1];
					isLeft = node->link[0] == child; 
					continue;
				}
				rotate(grandChild, child, node, isLeft);
				pthread_spin_unlock(&child->treeLock);
				child = grandChild;
			}
			
			if(parent == NULL)
				parent = lockParent(node);
			
			rotate(child, node, parent, isLeft ^ 0x0001);		
			bf = GET_BALANCE_FACTOR(node);
			if(bf >= 2 || bf <= -2){
				pthread_spin_unlock(&parent->treeLock);
				parent = child;
				child = NULL;
				isLeft = bf >= 2? 0: 1; 			// enforces to lock child
				continue;
			}
			avl_node_t *temp = child;
			child = node;
			node = temp;
			isLeft = node->link[0] == child;
			bf = GET_BALANCE_FACTOR(node);
		}

		if(child != NULL){
			pthread_spin_unlock(&child->treeLock);
		}
		child = node;
		node = parent != NULL? parent: lockParent(node);
		isLeft = node->link[0] == child;
		parent = NULL;	
	}

	if(child != NULL)
		pthread_spin_unlock(&child->treeLock);
	pthread_spin_unlock(&node->treeLock);
	if (parent != NULL) 
		pthread_spin_unlock(&parent->treeLock);
}

static void removeFromTree(avl_t *avl, avl_node_t *node, int hasTwoChildren, avl_node_t *parent, avl_node_t *node_to_delete)
{
	if(hasTwoChildren == 0){			//> node is a leaf or has one single child
		avl_node_t *child = (node->link[1] == NULL) ? node->link[0] : node->link[1];

		//> UpdateChild
		if(child != NULL)
			child->parent = parent;
		int isLeft = 0;
		if(parent->link[0] == node)
			isLeft = 1;
		if(isLeft)
			parent->link[0] = child;
		else
			parent->link[1] = child;

		node_to_delete = node;
		pthread_spin_unlock(&node->treeLock);
		rebalance(avl, parent, child, isLeft);
		return;
	}
		
	avl_node_t *succ = node->succ;		
	avl_node_t *oldParent = succ->parent;
	avl_node_t *oldRight = succ->link[1];		//> oldRight may be NULL

	//> UpdateChild
	if(oldRight != NULL)
		oldRight->parent = oldParent;
	int left = 0;
	if(oldParent->link[0] == succ)
		left = 1;
	if(left)
		oldParent->link[0] = oldRight;
	else
		oldParent->link[1] = oldRight;

	succ->leftHeight = node->leftHeight;
	succ->rightHeight = node->rightHeight;
	succ->parent = parent;
	succ->link[0] = node->link[0];
	succ->link[1] = node->link[1];
	node->link[0]->parent = succ;
	if(node->link[1] != NULL)		//> n.right  may be null
		node->link[1]->parent = succ;
	if(parent->link[0] == node)
		parent->link[0] = succ;
	else
		parent->link[1] = succ;

	int isLeft = 0;
	if(oldParent != node)
		isLeft = 1;
	int violated = abs(GET_BALANCE_FACTOR(succ)) >= 2; 
	if(!isLeft)
		oldParent = succ;
	else
		pthread_spin_unlock(&succ->treeLock);

	pthread_spin_unlock(&node->treeLock);
	node_to_delete = node;
	pthread_spin_unlock(&parent->treeLock);

	rebalance(avl, oldParent, oldRight, isLeft);	
	
	if(violated){
		pthread_spin_lock(&succ->treeLock);
		int bf = GET_BALANCE_FACTOR(succ);
		if(succ->valid && abs(bf) >= 2)
			rebalance(avl, succ, NULL, bf >= 2? 0: 1);	
		else
			pthread_spin_unlock(&succ->treeLock);
	}
	
	return;
}

static int _avl_lookup_helper(avl_t *avl, int key)
{ 
	int dir, currKey;
	avl_node_t *node, *child = NULL;	

	node = avl->root;
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
	
	return ((node->key == key) && node->valid);
}

static int _avl_insert_helper(avl_t *avl, avl_node_t *new_node)
{ 
	int inserted = 0;
	avl_node_t *node = NULL;
	int key = new_node->key;

	while(1){ 
		//> Searh operation
		int dir, currKey;
		avl_node_t *node, *child = NULL;
		node = avl->root;
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

		avl_node_t *p = (node->key >= key) ? node->pred : node;
		pthread_spin_lock(&p->succLock);
		avl_node_t *s = p->succ;  

		if((p->key < key) && (s->key >= key) && p->valid){

			if(s->key == key){			//> The key already exists -  Unsuccessful insert 
				pthread_spin_unlock(&p->succLock);
				return inserted; 	
			}

			//> Find the right parent for new node - ChooseParent
			avl_node_t *parent = ((node ==  p) || (node == s)) ? node : p;
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
			if(parent->key < key){			//> New_node is the right child
				parent->link[1] = new_node;
				parent->rightHeight = 1;
			}else{					//> New_node is the left child
				parent->link[0] = new_node;
				parent->leftHeight = 1;
			}

			if(parent != avl->root){
				avl_node_t *grandParent = lockParent(parent);
				rebalance(avl, grandParent, parent, grandParent->link[0] == parent); // !!!! SOSOOSOS arguments of rebalance
			}else{
				pthread_spin_unlock(&parent->treeLock);
			}

			inserted = 1;
			return inserted;			//> Successful insert					
		}
		pthread_spin_unlock(&p->succLock);		//> Validation failed - restart
	}
	return inserted;
}

static inline int _avl_delete_helper(avl_t *avl, int key, avl_node_t *node_to_delete)
{
	int ret = 0;

	while(1){ 
		//> Searh operation
		int dir, currKey;
		avl_node_t *node, *child = NULL;
		node = avl->root;
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

		avl_node_t *p = (node->key >= key) ? node->pred : node;
		pthread_spin_lock(&p->succLock);
		avl_node_t *s = p->succ;  

		if((p->key < key) && (s->key >= key) && p->valid){

			if(s->key > key){			//> The key doesn't exist -  Unsuccessful delete
				pthread_spin_unlock(&p->succLock);
				return ret; 	
			}

			pthread_spin_lock(&s->succLock);	//> Successful remove
			int hasTwoChildren = acquireTreeLocks(s);
			avl_node_t *sParent = lockParent(s);

			//> Update logical order
			s->valid = 0;
			avl_node_t *sSucc = s->succ;
			sSucc->pred = p;
			p->succ = sSucc;
			pthread_spin_unlock(&s->succLock);
			pthread_spin_unlock(&p->succLock);
	
			//> Physical remove
			removeFromTree(avl, s, hasTwoChildren, sParent, node_to_delete);
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
static int avl_violations, logic_violations;
static void _avl_validate(avl_node_t *root, int _th)
{
	if (root == NULL)
		return;

	avl_node_t *left = root->link[0];
	avl_node_t *right = root->link[1];

	total_nodes++;
	_th++;

	/* AVL violation? */
	if (left != NULL && left->key >= root->key)
		avl_violations++;
	if (right != NULL && right->key <= root->key)
		avl_violations++;

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
		_avl_validate(left, _th);
	}
	if (right != NULL){
		_avl_validate(right, _th);
	}
}


static inline int _avl_validate_helper(avl_node_t *root)
{
	int check_avl = 0, check_logic = 0;
	int check_rbt = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	avl_violations = 0;
	logic_violations = 0;

	_avl_validate(root, 0);

	check_avl = (avl_violations == 0);
	check_logic = (logic_violations == 0);
	check_rbt = (check_logic && check_avl);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  Valid Red-Black Tree: %s\n",
	       check_rbt ? "Yes [OK]" : "No [ERROR]");
	printf("  AVL Violation: %s\n",
	       check_avl ? "No [OK]" : "Yes [ERROR]");
	printf("  Logical Violation: %s\n",
	       check_logic ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size (Total): %8d\n",
	       total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_avl;
}

static inline int _avl_warmup_helper(avl_t *avl, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i, nodes_inserted = 0, ret = 0;
	avl_node_t *node;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		node = avl_node_new(key, NULL, NULL, NULL, NULL);

		ret = _avl_insert_helper(avl, node); 
		nodes_inserted += ret;

		if (!ret) {
			free(node);
		}
	}

	return nodes_inserted;
}

/********************************************************************************/
/* AVL (relaxed balanced) Logical Ordering Search tree interface implementation */
/********************************************************************************/
void *avl_new()
{
	printf("Size of tree node is %lu\n", sizeof(avl_node_t));
	return _avl_new_helper();
}

int avl_lookup(void *avl, void *thread_data, int key)
{
	int ret;

	ret = _avl_lookup_helper(avl, key);

	return ret;
}

int avl_insert(void *avl, void *thread_data, int key, void *value)
{
	int ret;
	avl_node_t *node;

	node = avl_node_new(key, value, NULL, NULL, NULL);

	ret = _avl_insert_helper(avl, node);

	if (!ret) {
		free(node);
	}

	return ret;
}

int avl_delete(void *avl, void *thread_data, int key)
{
	int ret;
	avl_node_t *node_to_delete=NULL;

	ret = _avl_delete_helper(avl, key, node_to_delete);

	if (ret) {
		free(node_to_delete);
	}

	return ret;
}

int avl_validate(void *avl)
{
	int ret;
	ret = _avl_validate_helper(((avl_t *)avl)->root);
	return ret;
}

int avl_warmup(void *avl, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret;
	ret = _avl_warmup_helper((avl_t *)avl, nr_nodes, max_key, seed, force);
	return ret;
}

char *avl_name()
{
	return "avl_logical_ordering";
}




