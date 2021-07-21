def format_arguments(blocks):
    if not blocks:
        return '()'

    args = ',\n'.join(map(str, blocks))
    args = '    '.join(args.splitlines(keepends=True))
    return f'(\n    {args}\n)'
