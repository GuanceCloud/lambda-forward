# 采集器「HuaweiCloud-FunctionGraph」配置手册

通过 华为云 中的 FunctionGraph 对 华为云 中的 LTS 日志数据进行抓取并上报到观测云日志中。

## 配置 FunctionGraph

### 使用控制台创建 FunctionGraph 函数

1.打开 FunctionGraph 控制台的函数页面。

2.选择创建函数

3.选择事件函数

4.输入函数名称。

5.设置 `运行时` 选项为 `Python 3.9`

6.在委托名称设置中选择LTS权限的委托。如果没有，可以点击创建委托，委托类型选择云服务/函数工作流 FunctionGraph，同时在授权记录中选择LTS的相关权限。

7.点击创建函数

8.在[GitHub](https://github.com/GuanceCloud/functiongraph-forward) 中拉取同步代码至下方代码源中，并添加DATAKIT为datakit部署地址，点击`Depoly`

### 配置 Lambda 触发器

1.点击 `添加触发器`

2.设置`触发器类型`为`云日志服务 (LTS)` 

3.选择需要监听的`日志组`，点击添加。

## X. 附录

### 操作所需最小权限

有权在当前区域和 华为云 账户中创建 LTS 日志组，以及创建日志流并将事件放入这些流中，所需权限列表

```
lts:*:get
lts:*:list
functiongraph:*:*
```
