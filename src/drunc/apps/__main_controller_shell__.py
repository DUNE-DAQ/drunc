
def main() -> None:
    from drunc.controller.interface.controller_shell import controller_shell, ControllerContext
    context = ControllerContext()
    try:
        controller_shell(obj = context)
    except Exception:
        from drunc.utils.utils import print_traceback
        print_traceback()



if __name__ == '__main__':
    main()
