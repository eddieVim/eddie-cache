# PaxosCacheKit

## 算法思想

算法思想来源于[Multi-Paxos](https://ongardie.net/static/raft/userstudy/paxossummary.pdf)

## 角色

- LeaderRole
- AcceptorRole
- FailureDetector



每个节点都会拥有这三者角色。

- LeaderRole：
    - 负责选举的发起
    - 提议（proposal）的发起、轮询
    - 提议的提交请求发起
- AcceptorRole：
    - 选举的投票
    - 提议的通过
    - 提议提交持久
- FailureDetector：
    - 检测下线情况，一有下线的节点即通知LeaderRole开始选举操作

