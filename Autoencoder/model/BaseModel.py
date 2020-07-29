import tensorflow.compat.v1 as tf
class BaseModel(object):
    def __init__(self,FLAGS):
        self.weight = tf.random_normal_initializer(mean=0.0, stddev = 0.2)
        self.bias = tf.zeros_initializer()
        self.FLAGS = FLAGS

    def _init_parameters(self):
        with tf.name_scope('weights'):
            self.W1 = tf.get_variable(name= "weight_1",shape= (self.FLAGS.num_v, 256),initializer = self.weight)
            self.W2 = tf.get_variable(name="weight_2", shape=(256, 128), initializer=self.weight)
            self.W3 = tf.get_variable(name="weight_3", shape=(128, 256), initializer=self.weight)
            self.W4 = tf.get_variable(name="weight_4", shape=(256, self.FLAGS.num_v), initializer=self.weight)
        with tf.name_scope('biases'):
            self.b1=tf.get_variable(name='bias_1', shape=(256), initializer=self.bias)
            self.b2=tf.get_variable(name='bias_2', shape=(128), initializer=self.bias)
            self.b3=tf.get_variable(name='bias_3', shape=(256), initializer=self.bias)

    def inference(self,x):
        with tf.name_scope('inference'):
            a1 = tf.nn.sigmoid(tf.nn.bias_add(tf.matmul(x, self.W1), self.b1))
            a2 = tf.nn.sigmoid(tf.nn.bias_add(tf.matmul(a1, self.W2), self.b2))
            a3 = tf.nn.sigmoid(tf.nn.bias_add(tf.matmul(a2, self.W3), self.b3))
            a4 = tf.matmul(a3, self.W4)
        return a4
