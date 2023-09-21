

def main():
    from drunc.controller.interface.controller import controller_cli
    try:
        controller_cli()
    except Exception:
        from drunc.utils.utils import print_traceback
        print_traceback()


if __name__ == '__main__':
    main()

