import json
import re
import requests


class ZhiHuSpider(object):
    def __init__(self, file_path):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36',
                        "Host": "www.zhihu.com",
                        "Referer": "https://www.zhihu.com/",
                        }
        # 建立一个会话，可以把同一用户的不同请求联系起来；直到会话结束都会自动处理cookies
        self.session = requests.Session()
        self.file_path = file_path

    def getQsAnswer(self, question_id):
        # 每次我们取10条回答
        limit = 10
        # 获取答案时的偏移量
        offset = 0
        # 开始时假设当前有这么多的回答，得到请求后我再解析
        total = 2 * limit
        # 我们当前已记录的回答数量
        record_num = 0
        # 定义问题的标题，为了避免获取失败，我们在此先赋值
        title = question_id
        # 存储所有的答案信息
        answer_info = []

        print('start spider question id: ' + question_id)
        while record_num < total:
            # 开始获取数据
            # https://www.zhihu.com/api/v4/questions/39162814/answers?
            # sort_by=default&include=data[*].is_normal,content&limit=5&offset=0
            url = 'https://www.zhihu.com/api/v4/questions/' \
                  + question_id + '/answers' \
                                  '?sort_by=default&include=data[*].is_normal,voteup_count,content' \
                                  '&limit=' + str(limit) + '&offset=' + str(offset)
            response = self.session.get(url, headers=self.headers)

            # 返回的信息为json类型
            response = json.loads(response.content)

            # 其中的paging实体包含了前一页&下一页的URL，可据此进行循环遍历获取回答的内容
            # 我们先来看下总共有多少回答
            total = response['paging']['totals']

            # 本次请求返回的实体信息数组
            datas = response['data']

            # 遍历信息并记录
            if datas is not None:

                if total <= 0:
                    break

                for data in datas:
                    dr = re.compile(r'<[^>]+>', re.S)
                    content = dr.sub('', data['content'])
                    # answer = data['author']['name'] + ' ' + str(data['voteup_count']) + ' 人点赞' + '\n'
                    answer = content + '\n'
                    answer_info.append('\n')
                    answer_info.append(answer)
                    answer_info.append('\n')
                    # 获取问题的title
                    title = data['question']['title']

                # 请求的向后偏移量
                offset += len(datas)
                record_num += len(datas)

                # 如果获取的数组size小于limit,循环结束
                if len(datas) < limit:
                    break

        print('spider end...')
        answer_info.insert(0, title + '\n')
        self.write2File(self.file_path, answer_info)

    def write2File(self, file_path, answer_info):
        print('Write info to file:Start...')
        # 将文件内容写到文件中
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines('\n\n')
            for text in answer_info:
                f.writelines(text)
            f.writelines('\n\n')
            print('Write info to file:end...')


if __name__ == '__main__':
    print("爬取速率不宜过高，目前没加入自动登录，频率过快可能被封IP")
    question_id = input('输入问题id；')
    file_path = input('输入存储文件目录：')
    try:
        spider = ZhiHuSpider(file_path)
        question_id = int(question_id)
        spider.getQsAnswer(question_id)
    except Exception as e:
        print("遇到异常，请检查问题id，文件目录等并重新运行...")
