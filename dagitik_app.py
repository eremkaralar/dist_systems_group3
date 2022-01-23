import sys
from PyQt5 import QtWidgets, QtCore
from dagitikproje_ui import Ui_MainWindow
import socket

class DagitikGui(QtWidgets.QMainWindow):
    def __init__(self, sock):
        self.qt_app = QtWidgets.QApplication(sys.argv)
        QtWidgets.QWidget.__init__(self, None)
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.sock=sock
        gelen=self.sock.recv(1024).decode()
        self.ui.textBrowser.append(str(gelen))

        self.ui.pushButton.pressed.connect(self.pushButtonPressed)
        

    

    def pushButtonPressed(self):
        self.ui.textBrowser.append(self.ui.lineEdit.text())
        self.sock.send(bytes(self.ui.lineEdit.text(), "utf-8"))
        self.ui.lineEdit.clear()
        gelen=self.sock.recv(1024).decode()
        self.ui.textBrowser.append(str(gelen))
        # self.ui.pushButton.pressed.disconnect()

        # self.ui.pushButton.setDisabled(True)

    def run(self):
        self.show()
        self.qt_app.exec_()
        

def main():
    host = "localhost"
    port = 12346
    s = socket.socket()
    s.connect((host, port))
    app = DagitikGui(s)
    app.run()
    
    

if __name__ == '__main__':
    main()
