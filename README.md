API接入文档
开启共享对接后，外部程序可以通过本接口拉取商品、读取库存、询价、下单以及查询订单。

基础说明
请求方式：POST
请求格式：推荐 application/x-www-form-urlencoded
鉴权方式：公共参数 app_id + sign
成功响应：code = 200
失败响应：通常为 code = 0，具体错误原因见 msg
签名算法
/**
 * 获取数据签名
 * @param array $data
 * @param string $appKey
 * @return string
 */
public static function generateSignature(array $data, $appKey): string
{
    unset($data['sign']);
    ksort($data);
    foreach ($data as $key => $val) {
        if ($val === '') {
            unset($data[$key]);
        }
    }
    return md5(urldecode(http_build_query($data) . "&key=" . (string)$appKey));
}
公共请求参数
参数名称	必选	类型	说明
app_id	是	string/int	商户ID，可在用户中心的商户资料中获取
sign	是	string	将本次请求所有POST参数按上方算法签名后的结果
app_key 是你的商户密钥，只用于本地生成签名，不建议直接上传到对方服务器。

如果请求里带了数组参数，例如 sku，签名时也必须把数组一起参与计算，并且要保证签名内容和实际提交内容完全一致。

公共响应格式
字段名称	类型	说明
code	int	状态码，成功时为 200
msg	string	提示信息，可选字段，只有控制器显式传入时才会返回
data	mixed	业务数据
成功示例：

{
  "code": 200,
  "data": {}
}
失败示例：

{
  "code": 0,
  "msg": "密钥错误"
}
业务字段说明
race：商品种类，对应商品配置中的 [category] 节点键名，例如 月卡、年卡
sku：商品SKU组合，建议按数组提交，例如 sku[机身颜色]=黑色&sku[存储容量]=256GB
card_id：预选卡ID，仅当商品详情中的 draft_status=1 时才有意义
widget：商品自定义控件，若商品详情返回了 widget，下单时需要把每个控件的 name 字段作为请求参数一起提交
request_no：请求幂等号，虽然代码里不是强制必填，但强烈建议每次下单都传唯一值，避免重复下单
获取全部商品列表
POST /shared/commodity/items

Body参数：无额外业务参数，只需要公共参数 app_id、sign

返回说明

data 为分类数组，每个分类下的 children 为商品列表，常见字段如下：

字段名称	类型	说明
id	int	分类ID或商品ID
name	string	分类名称或商品名称
children	array	当前分类下可对接商品列表
code	string	商品编码，下单和详情接口会用到
price	string/float	商品游客价
user_price	string/float	商品会员价/代理价
stock	int	自动发货商品会附带库存
delivery_way	int	发货方式
draft_status	int	是否支持预选
获取单个商品详情
POST /shared/commodity/item

Body参数
参数名称	必选	类型	说明
code	是	string	商品编码
返回核心字段
data 为商品详情对象，核心字段如下：

字段名称	类型	说明
id	int	商品ID
name	string	商品名称
description	string	商品介绍
code	string	商品编码
price	string/float	商品游客价
user_price	string/float	商品会员价/代理价
stock	int/string	当前库存
delivery_way	int	发货方式
contact_type	int	联系方式类型，0=不限、1=手机、2=邮箱、3=QQ
password_status	int	是否启用查单密码，0=否、1=是
draft_status	int	是否支持预选，0=否、1=是
draft_premium	string/float	预选附加价格
minimum	int	最低购买数量，0 表示不限制
maximum	int	单次最多购买数量，0 表示不限制
config	object	已解析后的商品配置，通常包含 category、sku、wholesale 等
widget	array/null	自定义控件配置，下单时要把控件的 name 对应值一起提交
seckill_status	int	是否秒杀商品
seckill_start_time	string	秒杀开始时间
seckill_end_time	string	秒杀结束时间
owner	object	供货商信息
service_url	string	客服链接
service_qq	string	客服QQ
share_url	string	商品分享链接
检查库存状态
POST /shared/commodity/inventoryState

Body参数
参数名称	必选	类型	说明
shared_code	是	string	商品编码
num	是	int	购买数量
card_id	否	int	预选卡ID，不预选时传 0 或不传
race	否	string	商品种类
返回说明
请求成功表示当前库存或预选卡状态满足下单条件，例如：

{
  "code": 200,
  "msg": "success",
  "data": []
}
如果库存不足、商品不存在、商品停售或预选卡已被占用，会直接返回失败信息。

获取库存与基础配置
POST /shared/commodity/inventory

Body参数
参数名称	必选	类型	说明
sharedCode	是	string	商品编码
race	否	string	商品种类，不传时按默认种类处理
返回核心字段
字段名称	类型	说明
count	int	当前库存数量
delivery_way	int	发货方式
draft_status	int	是否支持预选
price	string/float	商品基础售价
user_price	string/float	会员售价
config	string	商品配置，返回的是INI文本
factory_price	string/float	当前对接身份下的拿货价
is_category	bool	是否为种类商品
注意：这个接口返回的 config 是配置文本，不是 item 接口里那种已解析对象。

获取实时库存
POST /shared/commodity/stock

Body参数
参数名称	必选	类型	说明
code	是	string	商品编码
race	否	string	商品种类
sku	否	array	SKU组合
返回示例
{
  "code": 200,
  "data": {
    "stock": "15"
  }
}
询价
POST /shared/commodity/valuation

Body参数
参数名称	必选	类型	说明
code	是	string	商品编码
num	是	int	购买数量
race	否	string	商品种类
sku	否	array	SKU组合
card_id	否	int	预选卡ID
返回示例
{
  "code": 200,
  "data": {
    "price": "99.00"
  }
}
获取预选卡列表
POST /shared/commodity/draftCard

Body参数
参数名称	必选	类型	说明
code	是	string	商品编码
page	建议	int	页码，建议从 1 开始
limit	否	int	每页数量，默认 10
race	否	string	商品种类
sku	否	array	SKU组合
返回核心字段
字段名称	类型	说明
list	array	预选卡列表
total	int	总数量
list[].id	int	预选卡ID
list[].draft	string	预览信息
list[].draft_premium	string/float	该预选卡附加价格
只有当商品详情中的 draft_status=1 时，这个接口才可用。

获取单个预选卡详情
POST /shared/commodity/draft

Body参数
参数名称	必选	类型	说明
code	是	string	商品编码
card_id	是	int	预选卡ID
返回核心字段
字段名称	类型	说明
draft_premium	string/float	该预选卡附加价格
下单
POST /shared/commodity/trade

该接口内部强制使用余额支付

Body参数
参数名称	必选	类型	说明
shared_code	是	string	商品编码
num	是	int	购买数量
request_no	否	string	请求幂等号，建议每次下单都传唯一值
contact	否	string	联系方式，占位传值即可
race	否	string	商品种类
sku	否	array	SKU组合
card_id	否	int	预选卡ID
password	否	string	查单密码
coupon	否	string	优惠券代码
device	否	int	设备类型，未特殊区分时可传 0
如果商品详情返回了 widget，还需要把每个控件的 name 字段作为附加参数一起提交。

返回核心字段
字段名称	类型	说明
tradeNo	string	系统订单号
amount	string/float	实际扣费金额
secret	string/null	发货内容，若为手动发货则可能返回等待发货提示
stock	string/int	下单后的剩余库存
返回示例：

{
  "code": 200,
  "msg": "success",
  "data": {
    "url": null,
    "amount": "10.00",
    "tradeNo": "123260422101010888",
    "secret": "卡密内容",
    "stock": "14"
  }
}
订单查询
POST /shared/commodity/query

Body参数
参数名称	必选	类型	说明
tradeNo	是	string	系统订单号，注意这里字段名是 tradeNo，不是 trade_no
返回核心字段
字段名称	类型	说明
secret	string	发货内容或卡密内容
widget	object/null	下单时提交的自定义控件值
status	int	订单支付状态，0=未支付、1=已支付
对接建议流程
先调用 items 或 item 拉取商品信息。
如果商品有 category 或 sku，先确定 race 和 sku。
如果商品支持预选，先调用 draftCard 获取可选项，需要时再调用 draft 获取附加价格。
正式下单前，建议先调用 stock、valuation 或 inventoryState 做一次库存与价格确认。
调用 trade 下单。
如果你需要轮询发货结果，再调用 query 查询订单状态和发货内容。
补充说明
若只是测试鉴权是否可用，可额外请求 /shared/authentication/connect。
商品详情接口返回的 config 是解析后的对象，而 inventory 接口返回的 config 是INI文本，这不是文档写错，是当前程序本身的实现差异。
如果你打算完全兼容本项目自带共享客户端，建议优先按本文档中的字段名和返回结构实现，不要自行把 tradeNo 改成 trade_no 这类名字。
