# ReduceSideJoinSocialTriangle
CS6240
Spring 2019
HW-2 Reduce-Side Join Social Triangle

## 1) Pseudo code to determine cardinality(ALGORITHM):
In the reduce method,

    - Initialize counter1
    - For a value in values
          If the value starts with “l”, add it to “FollowedBy List”
          Else if the value starts with “r”, add it to the “Following list”
    - Counter1 = FollowedByList.size() * FollowingList.size()
    - Increment the global counter “CountStatistics” by this value “counter1”.

### Dataset: 50,000 users

### AWS runs
    
|   | Small cluster result(1 master, 5 slave)  | Large cluster results(1 master, 10 slave) |
| :------------ |:---------------:| -----:|
| RS-Join      | Running time: 36 minutes, Triangle count: 12029907 | Running time: 20 minutes, Triangle count: 12029907 |
