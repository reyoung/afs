// Docker registry client API
// 可以从一个docker image name+ tag中，获取有哪些layers，获取每层的元信息，以及下载每个层的数据
// 支持匿名访问、按 registry host 配置用户名密码登录（basic + bearer token 交换）和直接设置 bearer token。
package registry
