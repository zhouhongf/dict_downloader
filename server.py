from sanic import Sanic
from sanic import response
from database import MongoDatabase
from config import Config


db = MongoDatabase().db()
collection = db['english_dict']

# 初始化Sanic, app名称为url_server
app = Sanic(__name__)


@app.listener('after_server_stop')
async def server_stop(app, loop):
    print('========================= 【server】after_server_stop: do something ========================')


@app.listener('after_server_start')
async def server_start(app, loop):
    results = collection.find({'status': 'pending'})
    if results:
        for one in results:
            one['status'] = 'undo'
            collection.update_one({'_id': one['_id']}, {'$set': one})
    print('====== 【server】after_server_start: reset "pending" voice to "undo" in MongoDB ======')


@app.route('/fileurls')
async def task_get(request):
    count = request.args.get('count', 1000)
    count = int(count)
    # 随机取一定数量的记录，默认返回_id字段，设置仅返回manual_url字段
    select_command = [
        {'$match': {'status': 'undo'}},
        {'$sample': {'size': count}},
        {'$project': {'voice': 1}}
    ]

    task_list = []
    results = collection.aggregate(select_command)
    for one in results:
        if one['voice']:
            task = {'ukey': one['_id'], 'url': one['voice']}
            task_list.append(task)
            # 更新数据库中分发出去的记录为pending状态, 防止其他url_client重复下载
            one['status'] = 'pending'
            collection.update_one({'_id': one['_id']}, {'$set': one})
        else:
            one['status'] = 'done'
            collection.update_one({'_id': one['_id']}, {'$set': one})
    return response.json(task_list)


@app.route('/fileurls', methods=['POST', ])
async def task_post(request):
    one = request.json
    ukey = one['ukey']
    status_code = int(one['status_code'])

    result = collection.find_one({'_id': ukey})
    if status_code == 200:
        crawl_status = 'success'
    else:
        crawl_status = 'undo'
    result['status'] = crawl_status

    collection.update_one({'_id': ukey}, {'$set': result})
    print('【server】收到返回，已更新状态: %s' % one)
    return response.text('ok')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=Config.SERVER_PORT, workers=1, debug=False, access_log=False)




