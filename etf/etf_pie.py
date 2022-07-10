import matplotlib.pyplot as plt

import property_top_category

import yaml


plt.rcParams['font.sans-serif']=['WenQuanYi Micro Hei']

# mpl.rcParams['font.family'] = 'SimHei'# 同时绘制2个饼图
labels = ['损伤/中毒', u'呼吸系统疾病', u'肿瘤', u'心血管病', u'其他']
# summaryLabels = ['A股', '海外新兴市场', '黄金', '债券', '海外成熟市场']
summary= [70,20,1,5,4]
sizes1 = [5.90, 10.92, 26.11, 43.56, 13.51]
colors = ['orange','blueviolet','dodgerblue','red','green']

# 创建四个饼图型
fig, axs = plt.subplots(1, 2)

# 第一个饼图设置
patches, l_text, p_text = axs[0].pie(summary, labels=[e.value for e in property_top_category.PropertyTopCategory],
                                   labeldistance=1.2, autopct='%2.2f%%', shadow=False,
                                   startangle=140, pctdistance=0.8)

# 第二个饼图设置，设置第二个扇形偏移
patches, l_text, p_text = axs[1].pie(sizes1, labels=labels, labeldistance=1.1, autopct='%2.2f%%', shadow=False,
                                   startangle=140, pctdistance=0.6)

for t in l_text:
    t.set_size = 30
for t in p_text:
    t.set_size = 20
# 设置x，y轴刻度一致，这样饼图才能是圆的
plt.axis('equal')
plt.legend(loc='upper center', bbox_to_anchor=(-0.15, 1.1))

plt.grid()
# plt.show()


from pyecharts import options as opts
from pyecharts.charts import Pie
from pyecharts.commons.utils import JsCode


fn = """
    function(params) {
        if(params.name == '其他')
            return '\\n\\n\\n' + params.name + ' : ' + params.value + '%';
        return params.name + ' : ' + params.value + '%';
    }
    """


def new_label_opts():
    return opts.LabelOpts(formatter=JsCode(fn), position="center")


c = (
    Pie()
    .add(
        "",
        [list(z) for z in zip(["剧情", "其他"], [25, 75])],
        center=["20%", "30%"],
        radius=[60, 80],
        label_opts=new_label_opts(),
    )
    .add(
        "",
        [list(z) for z in zip(["奇幻", "其他"], [24, 76])],
        center=["55%", "30%"],
        radius=[60, 80],
        label_opts=new_label_opts(),
    )
    .add(
        "",
        [list(z) for z in zip(["爱情", "其他"], [14, 86])],
        center=["20%", "70%"],
        radius=[60, 80],
        label_opts=new_label_opts(),
    )
    .add(
        "",
        [list(z) for z in zip(["惊悚", "其他"], [11, 89])],
        center=["55%", "70%"],
        radius=[60, 80],
        label_opts=new_label_opts(),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="Pie-多饼图基本示例"),
        legend_opts=opts.LegendOpts(
            type_="scroll", pos_top="20%", pos_left="80%", orient="vertical"
        ),
    )
    .render("mutiple_pie.html")
)