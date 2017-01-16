# encoding: UTF-8

from __future__ import division

from datetime import datetime, timedelta
from collections import OrderedDict
from itertools import product
import pymongo

from ctaBase import *
from ctaSetting import *

from vtConstant import *
from vtGateway import VtOrderData, VtTradeData
from vtFunction import loadMongoSetting
from vtEngine import MainEngine


#----------------------------------------------------------------------
def main():
    """主程序入口"""
    # 重载sys模块，设置默认字符串编码方式为utf8
    reload(sys)
    sys.setdefaultencoding('utf8')

    # 初始化主引擎和主窗口对象
    mainEngine = MainEngine()
    mainEngine.connect('CTP')
    time.sleep(5)
    mainEngine.ctaEngine.loadSetting()
    mainEngine.ctaEngine.initStrategy('tradeTest')
    mainEngine.ctaEngine.startStrategy('tradeTest')
if __name__ == '__main__':
    main()