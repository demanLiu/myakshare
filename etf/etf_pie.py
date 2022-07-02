import matplotlib.pyplot as plt

import property_top_category

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
plt.show()