# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 15:06:36 2019

@author: lixiaopeng
"""

##台区线损率预测
#导入模块
import tensorflow as tf

from numpy.random import RandomState
import read_tfrecord

# 获取一层神经网络的权重，并将权重的L2正则化损失加入到集合中

n_input = 20

n_classes = 3


def get_weight(shape, lamda):
    # 定义变量

    var = tf.Variable(tf.random_normal(shape=shape), dtype=tf.float32)

    # 将变量的L2正则化损失添加到集合中

    tf.add_to_collection("losses", tf.contrib.layers.l2_regularizer(lamda)(var))

    return var


def affine(layer_dimension,x):
    # 获取神经网络的层数

    n_layers = len(layer_dimension)

    # 定义神经网络第一层的输入

    cur_layer = x

    # 当前层的节点个数

    in_dimension = layer_dimension[0]

    # 通过循环来生成5层全连接的神经网络结构

    for i in range(1, n_layers):
        # 定义神经网络上一层的输出，下一层的输入

        out_dimension = layer_dimension[i]

        # 定义当前层中权重的变量，并将变量的L2损失添加到计算图的集合中

        weight = get_weight([in_dimension, out_dimension], 0.001)

        # 定义偏置项

        bias = tf.Variable(tf.constant(0.1, shape=[out_dimension]))

        # 使用RELU激活函数
        cur_layer = tf.nn.relu(tf.matmul(cur_layer, weight) + bias)

        # 定义下一层神经网络的输入节点数

        in_dimension = layer_dimension[i]

    return cur_layer
def multilayer_perceptron(_X, _weights, mode='train'):
    bias = tf.Variable(tf.constant(0.1, shape=[3]))
    if mode == 'train':
        dropout_x = tf.nn.dropout(_X, 0.5)
        print(dropout_x.get_shape())
        print(_weights.get_shape())
        result =(tf.matmul(dropout_x, _weights) +bias)
    else:
        result=(tf.matmul(_X, _weights) + bias)



    return result

# 定义输入节点

x = tf.placeholder(tf.float32, shape=(None, n_input))

# 定义输出节点

y_ = tf.placeholder(tf.float32, shape=(None, n_classes))

# 定义每次迭代数据的大小

# 定义五层神经网络，并设置每一层神经网络的节点数目

layer_dimension = [20,512,1024,1024,512]


# 定义均方差的损失函数
hidden=affine(layer_dimension,x)
# mse_loss = tf.reduce_mean(tf.square(y_ - cur_layer))



# print(cur_layer.get_shape())

weights=get_weight([512,3],0.01)
pred_train = multilayer_perceptron(hidden, weights, )
pred_test = multilayer_perceptron(hidden, weights, mode='test')

# LOSS AND OPTIMIZER

cost_train = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=pred_train, labels=y_))
cost_test = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=pred_test, labels=y_))



# optm=tf.train.MomentumOptimizer(learning_rate=0.0001, momentum=0.9).minimize(cost)
corr_train = tf.equal(tf.argmax(pred_train, 1), tf.argmax(y_, 1))
corr_test = tf.equal(tf.argmax(pred_test, 1), tf.argmax(y_, 1))

accr_train = tf.reduce_mean(tf.cast(corr_train, "float"))
accr_test = tf.reduce_mean(tf.cast(corr_test, "float"))
# 将均方差孙函数添加到集合

tf.add_to_collection("losses", cost_train)

# 获取整个模型的损失函数,tf.get_collection("losses")返回集合中定义的损失

# 将整个集合中的损失相加得到整个模型的损失函数
loss = tf.add_n(tf.get_collection("losses"))

optm = tf.train.AdamOptimizer(learning_rate=0.01).minimize(loss)

if __name__ == '__main__':
    # INITIALIZER

    init = tf.global_variables_initializer()

    print("FUNCTIONS READY")

    training_epochs = 20

    batch_size = 64

    display_step = 1000

    # LAUNCH THE GRAPH

    sess = tf.Session()

    sess.run(init)

    # OPTIMIZE
    datas, labels = read_tfrecord.build_input(
        data_path=r'./data/normalization/train/train-*', batch_size=batch_size, dtype='float',
        num_class=n_classes)
    test_datas, test_labels = read_tfrecord.build_input(
        data_path=r'./data/normalization/validation/validation-*', batch_size=500, dtype='float',
        num_class=n_classes)

    with tf.Session() as sess:
        # init_op = tf.group(tf.global_variables_initializer(),tf.local_variables_initializer())
        sess.run(tf.group(tf.global_variables_initializer(), tf.local_variables_initializer()))
        # tf.global_variables_initializer().run()
        # tf.local_variables_initializer().run()
        coord = tf.train.Coordinator()
        threads = tf.train.start_queue_runners(sess=sess, coord=coord)
        for epoch in range(training_epochs):

            avg_cost = 0.

            total_batch = int(3800000/ batch_size)

            # ITERATION

            for i in range(total_batch):
                x_batch, y_batch = sess.run([datas, labels])
                feeds_train = {x: x_batch, y_: y_batch}
                _, loss_train= sess.run([optm, loss], feed_dict=feeds_train)
                #
                # # avg_cost += sess.run(cost, feed_dict=feeds)
                #
                # # DISPLAY
                if i % display_step == 0:
                    print("Epoch: %03d/%03d batch_num: %04d cost: %.9f" % (epoch, training_epochs, i, loss_train))
                    # feeds_train = {x: x_batch, y_: y_batch}
                    train_acc = sess.run(accr_train, feed_dict=feeds_train)
                    print("TRAIN ACCURACY: %.3f" % (train_acc))

                    # avg_cost = avg_cost / total_batch

                    test_x_batch, test_y_batch = sess.run([test_datas, test_labels])
                    feeds_test = {x: test_x_batch, y_: test_y_batch}
                    loss_test,test_acc = sess.run([cost_test,accr_test], feed_dict=feeds_test)
                    print("Epoch: %03d/%03d batch_num: %04d cost: %.9f" % (epoch, training_epochs, i, loss_test))

                    print("TEST ACCURACY: %.3f" % (test_acc))

        print("OPTIMIZATION FINISHED")
        coord.request_stop()
        coord.join(threads)
        import pandas as pd

df = pd.read_csv('./data/normalization/hf_test.csv')

labels = df['XSL'].as_matrix().reshape([-1,1])
datas=df[[ 'FHL', 'AVG_FZL', 'dow', 'month',
       'season', 'tg_cltype', 'tg_DATATYPE', 'swgf_type', 'Vtype', 'area_type',
       'CAL_SUM_scaled', 'AVG_TEMP_scaled', 'doy_scaled',
       'day_scaled', 'CHG_DATE_scaled', 'TG_CAP_scaled', 'machine_tag_scaled',
       'CD_TOTAL_scaled', 'PER_CAP_scaled', 'tg_usertype_scaled', 't_24']].as_matrix()
# print(labels)

def save(lables,precetion):
    df1=pd.DataFrame({'真实值':lables,"预测值":precetion})
    df1.to_csv('./test.csv')
import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'



"""
读取数据
"""
def build_input(data_path, batch_size,num_class,dtype='int'):
    #读取一个文件夹下匹配的文件
    files = tf.train.match_filenames_once(data_path)
    #把文件放入文件队列中
    filename_queue = tf.train.string_input_producer(files, shuffle=True)
    # #创建一个reader，
    reader = tf.TFRecordReader()
    # #从文件中读取一个样例。也可以使用read_up_to函数一次性读取多个样例
    _, serialized_example = reader.read(filename_queue)
    # #解析一个样本
    features = tf.parse_single_example(
          serialized_example,
        features={
            "tg_no": tf.FixedLenFeature([], tf.string),
            "date": tf.FixedLenFeature([], tf.string),
            "xsl": tf.FixedLenFeature([], tf.string),
            "data": tf.FixedLenFeature([], tf.string),
            "label": tf.FixedLenFeature([], tf.int64),
        }

        )

    # 组合样例中队列最多可以存储的样例个数
    capacity = 500+3*batch_size
    #读取一个样例中的特征
    data,label = features['data'],features['label']
    tg_no,date,xsl = features['tg_no'],features['date'],features['xsl']



    # ###tf.image.decode_jpeg#############
    # image_raw = tf.image.decode_jpeg(image, channels=3)
    # retyped_height = tf.cast(height, tf.int32)
    # retyped_width = tf.cast(width,tf.int32)
    # retyped_channel = tf.cast(channel,tf.int32)
    data=tf.decode_raw(data, tf.float64)
    data = tf.reshape(data, [21])
    data=tf.cast(data,tf.float32)

    xsl = tf.decode_raw(xsl, tf.float64)
    xsl = tf.reshape(xsl, [1])
    xsl =tf.cast(xsl,tf.float32)
    label = tf.cast(label,tf.int32)
    # data = tf.cast(data,dtype=tf.float32)
    # # image_resize = tf.image.resize_images(image_raw,[32,32],method=np.random.randint(4))
    # if dtype == 'float':
    #     distorted_image = preprocess_for_train(image_raw, resize, resize, None)
    #     # 组合样例两种方法一种是tf.train.batch;另一种是tf.train.shuffle_batch，输入的shape一定要明确
    #     images, labels = tf.train.shuffle_batch([distorted_image, labels], batch_size=batch_size,
    #                                                         capacity=capacity, min_after_dequeue=500)
    # else:
    #     image_resize=tf.image.resize_image_with_crop_or_pad(image_raw, resize, resize)
    #     images,labels= tf.train.shuffle_batch([image_resize,labels ],batch_size=batch_size,capacity=capacity,min_after_dequeue=500)
    datas, xsl= tf.train.shuffle_batch([data, xsl], batch_size=batch_size,capacity=capacity, min_after_dequeue=500)
    # xsls = tf.reshape(xsl, [batch_size, 1])
    # indices = tf.reshape(tf.range(0, batch_size, 1), [batch_size, 1])
    # labels = tf.sparse_to_dense(
    #     tf.concat(values=[indices, labels], axis=1),
    #     [batch_size, num_class], 1.0, 0.0)
    # tf.summary.image('images', images)
    return  datas,xsl





if __name__ == '__main__':
    data_path = './data/normalization/validation/validation-*'
    dates,xsls = build_input(data_path, batch_size=4, num_class=3, dtype='float')
    # def show(image):
    #     plt.imshow(image)
    #     plt.show()

    with tf.Session() as sess:
        # init_op = tf.group(tf.global_variables_initializer(),tf.local_variables_initializer())
        sess.run( tf.group(tf.global_variables_initializer(),tf.local_variables_initializer()))
        # tf.global_variables_initializer().run()
        # tf.local_variables_initializer().run()
        coord=tf.train.Coordinator()
        threads = tf.train.start_queue_runners(sess=sess,coord=coord)

        for i in range(1):
            date_batch,xsls_batch= sess.run([dates,xsls])
            # print(tg_no_batch,"\n",date_batch,"\n",cur_example_batch,"\n",cur_label_batch)
            print(date_batch,"\n",xsls_batch,"\n")
            # for image in cur_example_batch:
            #     show(image)
        coord.request_stop()
        coord.join(threads)
from collections import namedtuple

import numpy as np
import tensorflow as tf
import six

from tensorflow.python.training import moving_averages


HParams = namedtuple('HParams',
                     'batch_size, num_classes, min_lrn_rate, lrn_rate,keep_prob, '
                     'num_nn_units, use_bottleneck, weight_decay_rate, '
                     'relu_leakiness, optimizer')


class NNet(object):
  """ResNet model."""

  def __init__(self, hps, datas, labels, mode):
    """ResNet constructor.

    Args:
      hps: Hyperparameters.
      datas: Batches of datas. [batch_size, feature_size]
      labels: Batches of labels 类别标签. [batch_size, num_classes]
      mode: One of 'train' and 'eval'.
    """
    self.hps = hps
    self._datas = datas
    self.labels = labels
    self.mode = mode
    self._extra_train_ops = []


  # 构建模型图
  def build_graph(self):
    # 新建全局step
    self.global_step = tf.contrib.framework.get_or_create_global_step()
    # 构建NNet网络模型
    self._build_model()
    # 构建优化训练操作
    if self.mode == 'train':
      self._build_train_op()
    # 合并所有总结
    self.summaries = tf.summary.merge_all()


  # 构建模型
  def _build_model(self):
    with tf.variable_scope('init'):
        # x shape:[batch_size,feature_size]
        x = self._datas
        """第一层参数（21X10000）"""
        x = self._NN('unit_1', x,self.hps.num_nn_units[0])
        x = self._relu(x, self.hps.relu_leakiness)



        for i in six.moves.range(1, len(self.hps.num_nn_units)):
          with tf.variable_scope('unit_2_%d' % i):
            # shape = (batch_size,10000)
            x = self._NN('unit_%d' %(i+1),x,self.hps.num_nn_units[i])
            x = self._relu(x, self.hps.relu_leakiness)

            # if self.mode == 'train':
          with tf.variable_scope('dropout_%d'%(i+1)):
            #dropout层
            x=tf.nn.dropout(x,self.hps.keep_prob)
            # 全连接层 + Softmax
        with tf.variable_scope('logit'):
            self.logits = self._fully_connected(x, self.hps.num_classes)
            # self.predictions = tf.nn.softmax(logits)

        # 构建损失函数
        with tf.variable_scope('costs'):
              # 交叉熵
              # xent = tf.nn.softmax_cross_entropy_with_logits(
              #     logits=logits, labels=self.labels)
              # self.cost = tf.reduce_mean(xent, name='xent')
              # 加和
              self.cost =tf.losses.mean_squared_error(labels=self.labels, predictions=self.logits)
              #加权损失,给与预测低的线损率更大的损失值,效果预测的线损率要比标准的线损率较高
              # self.cost=tf.reduce_sum(tf.where(tf.greater(self.logits,self.labels),(self.logits-self.labels)*1,(self.labels-self.logits)*3))
              # L2正则，权重衰减
              self.cost += self.decay()
              # 添加cost总结，用于Tensorborad显示
              tf.summary.scalar('cost', self.cost)


  # 构建训练操作
  def _build_train_op(self):
    # 学习率/步长
    self.lrn_rate = tf.constant(self.hps.lrn_rate, tf.float32)
    #指数衰减的学习率
    # self.lrn_rate =tf.train.exponential_decay(self.hps.lrn_rate,self.global_step,1000,0.96,staircase=True)
    tf.summary.scalar('learning_rate', self.lrn_rate)

    # 计算训练参数的梯度
    trainable_variables = tf.trainable_variables()
    grads = tf.gradients(self.cost, trainable_variables)

    # 设置优化方法
    if self.hps.optimizer == 'sgd':
      optimizer = tf.train.GradientDescentOptimizer(self.lrn_rate)
    elif self.hps.optimizer == 'mom':
      optimizer = tf.train.MomentumOptimizer(self.lrn_rate, 0.9)
    else:
      optimizer = tf.train.AdamOptimizer(self.lrn_rate)
    # 梯度优化操作
    apply_op = optimizer.apply_gradients(
                        zip(grads, trainable_variables),
                        global_step=self.global_step,
                        name='train_step')

    # 合并BN更新操作
    train_ops = [apply_op]
    # 建立优化操作组
    self.train_op = tf.group(*train_ops)




  # 权重衰减，L2正则loss
  def decay(self):
    costs = []
    # 遍历所有可训练变量
    for var in tf.trainable_variables():
      #只计算标有“weight”的变量
      if var.op.name.find(r'weight') > 0:
        costs.append(tf.nn.l2_loss(var))
    # 加和，并乘以衰减因子
    return tf.multiply(self.hps.weight_decay_rate, tf.add_n(costs))

  # 神经元
  def _NN(self, name, x, units_num):
    with tf.variable_scope(name):
      feature_size = x.get_shape()[1]
      # 获取或先建参数，正态随机初始化
      w = tf.get_variable(
          'weight',
          [feature_size, units_num],
          tf.float32,
          initializer=tf.truncated_normal_initializer(0.0, stddev=0.1))
      b= tf.get_variable('biases',[units_num],tf.float32,initializer=tf.constant_initializer())
      # 计算中间网络
      return tf.add(tf.matmul(x,w),b)

  # leaky ReLU激活函数，泄漏参数leakiness为0就是标准ReLU
  def _relu(self, x, leakiness=0.0):
        # return tf.where(tf.less(x, 0.0), leakiness * x, x, name='leaky_relu')
        return tf.nn.relu(x)

  # 全连接层，网络最后一层
  def _fully_connected(self, x, out_dim):
    # 输入转换成2D tensor，尺寸为[N,-1]
    x = tf.reshape(x, [self.hps.batch_size, -1])
    # 参数w，平均随机初始化，[-sqrt(3/dim), sqrt(3/dim)]*factor
    w = tf.get_variable('weight', [x.get_shape()[1], out_dim], tf.float32,
                        initializer=tf.truncated_normal_initializer(0.0, stddev=0.1))
    # 参数b，0值初始化
    b = tf.get_variable('biases', [out_dim], tf.float32, initializer=tf.constant_initializer())
    # 计算x*w+b
    return tf.nn.xw_plus_b(x, w,b)
"""NN Train/Eval module.
"""
import time
import six
import sys
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import read_tfrecord
import numpy as np
import NN_mode
import tensorflow as tf

# FLAGS参数设置
FLAGS = tf.app.flags.FLAGS

# 模式：输入数据为整型，还是浮点型
tf.app.flags.DEFINE_string('mode',
                           'train',
                           'train or val.')
# 训练数据路径
tf.app.flags.DEFINE_string('train_data_path', 
                           './data/normalization/train/train-*',
                           'Filepattern for training data.')
# # 测试数据路劲
# tf.app.flags.DEFINE_string('eval_data_path',
#                            './data/undersample/validation/validation-.tfrecords',
#                            'Filepattern for eval data')
# 图片尺寸
tf.app.flags.DEFINE_integer('features_size',
                            21,
                            'num of feature.')
# 训练过程数据的存放路劲
tf.app.flags.DEFINE_string('train_dir', 
                           'temp/train',
                           'Directory to keep training outputs.')
# 测试过程数据的存放路劲
tf.app.flags.DEFINE_string('eval_dir', 
                           'temp/eval',
                           'Directory to keep eval outputs.')
# 测试数据的Batch数量
tf.app.flags.DEFINE_integer('eval_batch_count', 
                            500,
                            'Number of batches to eval.')
# 一次性测试
tf.app.flags.DEFINE_bool('eval_once', 
                         False,
                         'Whether evaluate the model only once.')
# 模型存储路劲
tf.app.flags.DEFINE_string('log_root', 
                           'temp',
                           'Directory to keep the checkpoints. Should be a '
                           'parent directory of FLAGS.train_dir/eval_dir.')
# GPU设备数量（0代表CPU）
tf.app.flags.DEFINE_integer('num_gpus', 
                            0,
                            'Number of gpus used for training. (0 or 1)')


def train(hps):
  # 构建输入数据(读取队列执行器）
  datas, labels = read_tfrecord.build_input(
      data_path=FLAGS.train_data_path, batch_size=hps.batch_size,dtype='float',num_class=hps.num_classes)
  # 构建残差网络模型
  model = NN_mode.NNet(hps, datas, labels,FLAGS.mode)
  model.build_graph()

  # 计算预测准确率
  truth = tf.argmax(model.labels, axis=1)
  # predictions = tf.argmax(model.predictions, axis=1)
  # precision = tf.reduce_mean(tf.to_float(tf.equal(predictions, truth)))

  # 建立总结存储器，每100步存储一次
  summary_hook = tf.train.SummarySaverHook(
              save_steps=100,
              output_dir=FLAGS.train_dir,
              summary_op=tf.summary.merge(
                              [model.summaries,
                               tf.summary.scalar('MSE', model.cost)]))


  # 建立日志打印器，每100步打印一次
  logging_hook = tf.train.LoggingTensorHook(
      tensors={'step': model.global_step,
               'loss': model.cost,
                'lrn-rate':model.lrn_rate,
               'l2':model.decay(),
               'mse':model.cost-model.decay()},
      every_n_iter=100)

  # 学习率更新器，基于全局Step
  class _LearningRateSetterHook(tf.train.SessionRunHook):

    def begin(self):
      #初始学习率
      self._lrn_rate =0.001


    def before_run(self, run_context):
      return tf.train.SessionRunArgs(
                      # 获取全局Step
                      model.global_step,
                      # 设置学习率
                      feed_dict={model.lrn_rate: self._lrn_rate})  

    def after_run(self, run_context, run_values):
      # 动态更新学习率
      train_step = run_values.results
      if train_step < 200000:
        self._lrn_rate = 0.001
      elif train_step < 400000:
        self._lrn_rate = 0.0001
      elif train_step < 800000:
        self._lrn_rate = 0.00001
      else:
        self._lrn_rate = 0.000001

  # 建立监控Session
  with tf.train.MonitoredTrainingSession(
      checkpoint_dir=FLAGS.log_root,
      hooks=[logging_hook, _LearningRateSetterHook()],
      chief_only_hooks=[summary_hook],
      # 禁用默认的SummarySaverHook，save_summaries_steps设置为0
      save_summaries_steps=0, 
      config=tf.ConfigProto(allow_soft_placement=True)) as mon_sess:
    while not mon_sess.should_stop():
      # 执行优化训练操作
      mon_sess.run(model.train_op)





def main(_):
  # 设备选择
  if FLAGS.num_gpus == 0:
    dev = '/cpu:0'
  elif FLAGS.num_gpus == 1:
    dev = '/gpu:0'
  else:
    raise ValueError('Only support 0 or 1 gpu.')
    
  # 执行模式
  if FLAGS.mode == 'train':
    batch_size = 128
  elif FLAGS.mode == 'eval':
    batch_size = 512


  # 残差网络模型参数
  hps = NN_mode.HParams(batch_size=batch_size,
                        num_classes=1,
                        min_lrn_rate=0.000001,
                        lrn_rate=0.001,
                        keep_prob=0.7,
                        num_nn_units=[512,1024,2048,2048,1024,512],
                        use_bottleneck=False,
                        weight_decay_rate=0.0002,
                        relu_leakiness=0.0,
                        optimizer='adm')
  # 执行训练或测试
  with tf.device(dev):
    if FLAGS.mode == 'train':
      train(hps)



if __name__ == '__main__':
  tf.logging.set_verbosity(tf.logging.INFO)
  tf.app.run()
"""NN Train/Eval module.
"""
import time
import six
import sys

import read_tfrecord
import numpy as np
import NN_mode
import tensorflow as tf
from test_data import datas
from test_data import labels
from test_data import save


# FLAGS参数设置
FLAGS = tf.app.flags.FLAGS

# 模式：输入数据为整型，还是浮点型
tf.app.flags.DEFINE_string('mode',
                           'test',
                           'train or val.')
# 训练数据路径
# tf.app.flags.DEFINE_string('train_data_path',
#                            './data/train/train-*',
#                            'Filepattern for training data.')
# 测试数据路劲
# tf.app.flags.DEFINE_string('test_data_path',
#                            './data/normalization/validation/validation-*',
#                            'Filepattern for eval data')
# 图片尺寸
tf.app.flags.DEFINE_integer('features_size',
                            21,
                            'num of feature.')
# 训练过程数据的存放路劲
tf.app.flags.DEFINE_string('train_dir', 
                           'temp/train',
                           'Directory to keep training outputs.')
# 测试过程数据的存放路劲
tf.app.flags.DEFINE_string('eval_dir', 
                           'temp/eval',
                           'Directory to keep eval outputs.')
# 测试数据的Batch数量
tf.app.flags.DEFINE_integer('eval_batch_count', 
                            8,
                            'Number of batches to eval.')
# 一次性测试
tf.app.flags.DEFINE_bool('eval_once', 
                         True,
                         'Whether evaluate the model only once.')
# 模型存储路劲
tf.app.flags.DEFINE_string('log_root', 
                           'temp',
                           'Directory to keep the checkpoints. Should be a '
                           'parent directory of FLAGS.train_dir/eval_dir.')
# GPU设备数量（0代表CPU）
tf.app.flags.DEFINE_integer('num_gpus', 
                            0,
                            'Number of gpus used for training. (0 or 1)')


def test(hps,datas,labels):
  # 构建输入数据(读取队列执行器）
  # 构建残差网络模型
  datas =tf.constant(datas,tf.float32)
  labels = tf.constant(labels,tf.float32)
  model = NN_mode.NNet(hps, datas, labels, FLAGS.mode)
  model.build_graph()
  # 模型变量存储器
  saver = tf.train.Saver()
  # 总结文件 生成器
  # summary_writer = tf.summary.FileWriter(FLAGS.eval_dir)
  init = (tf.global_variables_initializer(), tf.local_variables_initializer())
  # 执行Session
  sess = tf.Session(config=tf.ConfigProto(allow_soft_placement=True))
  sess.run(init)
  # 启动所有队列执行器
  # tf.train.start_queue_runners(sess)
  # best_precision = 0.0
  best_cost = 10.0
  while True:
    # 检查checkpoint文件
    try:
        ckpt_state = tf.train.get_checkpoint_state(FLAGS.log_root)
    except tf.errors.OutOfRangeError as e:
      tf.logging.error('Cannot restore checkpoint: %s', e)
      continue
    if not (ckpt_state and ckpt_state.model_checkpoint_path):
      tf.logging.info('No model to eval yet at %s', FLAGS.log_root)
      continue

    # 读取模型数据(训练期间生成)
    tf.logging.info('Loading checkpoint %s', ckpt_state.model_checkpoint_path)
    saver.restore(sess, ckpt_state.model_checkpoint_path)
    # 逐Batch执行测试
    # total_prediction, correct_prediction = 0, 0
    total_prediction,current_cost=0,0

      # 执行预测
    (loss, predictions, truth, train_step) = sess.run([model.cost, model.logits,model.labels, model.global_step])
      # 计算预测结果
      # truth = np.argmax(truth, axis=1)
      # predictions = np.argmax(predictions, axis=1)
      # correct_prediction += np.sum(truth == predictions)

      # total_prediction += predictions.shape[0]

    # 计算准确率

    # print(sess.run(tf.shape(truth)))
    # print(sess.run(tf.shape(predictions)))
    save(sess.run(tf.reshape(truth,(-1,))),sess.run(tf.reshape(predictions,(-1,))))
    # precision = 1.0 * correct_prediction / total_prediction

    # 添加准确率总结
    # precision_summ = tf.Summary()
    # precision_summ.value.add(
    #     tag='loss', simple_value=total_cost)
    # summary_writer.add_summary(precision_summ, train_step)
    
    # 添加最佳准确总结
    # best_precision_summ = tf.Summary()
    # best_precision_summ.value.add(
    #     tag='Best Cost', simple_value=best_cost)
    # summary_writer.add_summary(best_precision_summ, train_step)
    
    # 添加测试总结
    #summary_writer.add_summary(summaries, train_step)
    
    # 打印日志
    tf.logging.info('loss: %.3f'%loss)


    # 执行写文件
    # summary_writer.flush()

    if FLAGS.eval_once:
      break

    time.sleep(10)


def main(_):
  # 设备选择
  if FLAGS.num_gpus == 0:
    dev = '/cpu:0'
  elif FLAGS.num_gpus == 1:
    dev = '/gpu:0'
  else:
    raise ValueError('Only support 0 or 1 gpu.')

  # # 执行模式
  # if FLAGS.mode == 'train':
  #   batch_size = 256
  # elif FLAGS.mode == 'eval':
  #   batch_size = 500


  # 残差网络模型参数
  hps = NN_mode.HParams( batch_size = datas.shape[0],
                        num_classes=1,
                        min_lrn_rate=0.0001,
                        lrn_rate=0.001,
                        keep_prob=1.0,
                        num_nn_units=[512,1024,2048,2048,1024,512],
                        use_bottleneck=False,
                        weight_decay_rate=0.00,
                        relu_leakiness=0.0,
                        optimizer='sgd')
  # 执行训练或测试
  test(hps,datas,labels)



if __name__ == '__main__':
  tf.logging.set_verbosity(tf.logging.INFO)
  tf.app.run()
import tensorflow as tf
import pandas as pd
v=tf.constant([[2],[3],[4],[5]],dtype=tf.float32)
v1=tf.reshape(v,(-1,))
mode=tf.placeholder('string',name='model')
with tf.Session() as sess:
    print(pd.DataFrame({"val":sess.run(v1)}))
    print(sess.run(mode,feed_dict={mode:'train'}))
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
#画图显示中文
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
plt.rcParams['figure.figsize'] = (20.0, 12.5) 
df=pd.read_csv(r'../test.csv',encoding='gbk',names=['index','truth','predictions'],header=0)
df1=df.iloc[1:100,:]
plt.figure()
labels=['真实值','预测值']
# power_smooth = spline(df['Step'],df['train_loss'],xnew)
plt.plot(df1['index'] ,df1['truth'],'-' ,color='green',linewidth=2.5,ms=15,marker='*', mec='g')
plt.plot(df1['index'],df1['predictions'], '--^', color='blue',linewidth=2.5,ms=15,mec='b')
plt.yticks(fontsize=16)
plt.xticks(fontsize=16)
plt.ylim(0, 12)
# plt.xlim(10000, 704800)
plt.xticks(rotation=40)
new_ticks = np.linspace(0,12,13)
plt.yticks(new_ticks,fontsize=16)
plt.xlabel('样本编号',fontsize=20)
plt.ylabel('线损率(%)',fontsize=20)
plt.legend(fontsize=16,labels=labels,loc='best')
plt.savefig(r'../prediction.png',dpi=300,bbox_inches = 'tight')
plt.show()
import pandas as pd
df = pd.read_csv('../data/normalization/hf_normallation.csv')
df[(df['TG_NO']==120666107)&(df['REPORT_DATE']=='2018-09-28')]
df.columns
len(df)
df[df['t_24'].isnull()]






