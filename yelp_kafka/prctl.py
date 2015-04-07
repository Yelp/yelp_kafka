import ctypes

libc = ctypes.CDLL('libc.so.6')

# int prctl(int option, unsigned long arg2, unsigned long arg3,
#           unsigned long arg4, unsigned long arg5);
libc.prctl.restype = ctypes.c_int
libc.prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                       ctypes.c_ulong, ctypes.c_ulong]

prctl = libc.prctl

# Values to pass as first argument to prctl()

PR_SET_NAME = 15  # Set process name
PR_GET_NAME = 16  # Get process name
