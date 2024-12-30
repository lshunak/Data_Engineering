import sys

def print_cmd_line_args_reverse():
    args = sys.argv[1:]
    args.reverse()
    print(args)

if __name__ == '__main__':
    print_cmd_line_args_reverse()