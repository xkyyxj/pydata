# 3rd party modules
import threading
from time import sleep

from win32api import GetModuleHandle
from win32api import PostQuitMessage
from win32con import CW_USEDEFAULT
from win32con import IDI_APPLICATION
from win32con import IMAGE_ICON
from win32con import LR_DEFAULTSIZE
from win32con import LR_LOADFROMFILE
from win32con import WM_DESTROY
from win32con import WM_USER
from win32con import WS_OVERLAPPED
from win32con import WS_SYSMENU
from win32gui import CreateWindowEx
from win32gui import DestroyWindow
from win32gui import LoadIcon
from win32gui import LoadImage
from win32gui import NIF_ICON
from win32gui import NIF_INFO
from win32gui import NIF_MESSAGE
from win32gui import NIF_TIP
from win32gui import NIM_ADD
from win32gui import NIM_DELETE
from win32gui import NIM_MODIFY
from win32gui import RegisterClass
from win32gui import UnregisterClass
from win32gui import Shell_NotifyIcon
from win32gui import UpdateWindow
from win32gui import WNDCLASS


class ToastNotifier(object):
    """Create a Windows 10  toast notification.

    from: https://github.com/jithurjacob/Windows-10-Toast-Notifications
    """

    def __init__(self):
        """Initialize."""
        self._thread = None

    def _show_toast(self, title, msg,
                    icon_path, duration):
        """Notification settings.

        :title: notification title
        :msg: notification message
        :icon_path: path to the .ico file to custom notification
        :duration: delay in seconds before notification self-destruction
        """
        message_map = {WM_DESTROY: self.on_destroy, }

        # Register the window class.
        self.wc = WNDCLASS()
        self.hinst = self.wc.hInstance = GetModuleHandle(None)
        self.wc.lpszClassName = str("PythonTaskbar")  # must be a string
        self.wc.style = self.wc.cbWndExtra = 0
        # self.wc.lpfnWndProc = message_map  # could also specify a wndproc.
        try:
            self.classAtom = RegisterClass(self.wc)
        except:
            pass  # not sure of this
        style = WS_OVERLAPPED | WS_SYSMENU
        self.hwnd = CreateWindowEx(0, "PythonTaskbar", "555", style,
                                   0, 0, CW_USEDEFAULT,
                                   CW_USEDEFAULT,
                                   0, 0, self.hinst, None)
        UpdateWindow(self.hwnd)

        hicon = LoadIcon(0, IDI_APPLICATION)

        # Taskbar icon
        flags = NIF_ICON | NIF_TIP
        nid = (self.hwnd, 0, flags, WM_USER + 20, hicon, "66666")
        Shell_NotifyIcon(NIM_ADD, nid)
        Shell_NotifyIcon(NIM_MODIFY, (self.hwnd, 0, NIF_INFO,
                                      WM_USER + 20,
                                      hicon, "289347293874", msg, 0,
                                      title))
        # # take a rest then destroy
        sleep(duration)
        DestroyWindow(self.hwnd)
        UnregisterClass(self.wc.lpszClassName, None)
        return None

    def show_toast(self, title="Notification", msg="Here comes the message",
                   icon_path=None, duration=5, threaded=False):
        """Notification settings.

        :title: notification title
        :msg: notification message
        :icon_path: path to the .ico file to custom notification
        :duration: delay in seconds before notification self-destruction
        """
        if not threaded:
            self._show_toast(title, msg, icon_path, duration)
        else:
            if self.notification_active():
                # We have an active notification, let is finish so we don't spam them
                return False

            self._thread = threading.Thread(target=self._show_toast, args=(title, msg, icon_path, duration))
            self._thread.start()
        return True

    def notification_active(self):
        """See if we have an active notification showing"""
        if self._thread != None and self._thread.is_alive():
            # We have an active notification, let is finish we don't spam them
            return True
        return False

    def on_destroy(self, hwnd, msg, wparam, lparam):
        """Clean after notification ended.

        :hwnd:
        :msg:
        :wparam:
        :lparam:
        """
        print("destroy how many?????")
        nid = (self.hwnd, 0)
        Shell_NotifyIcon(NIM_DELETE, nid)
        PostQuitMessage(0)

        return None


toaster = ToastNotifier()
toaster.show_toast("12", "1233", duration=5)
toaster.show_toast("4444", "14444444233", duration=5)
sleep(100)
