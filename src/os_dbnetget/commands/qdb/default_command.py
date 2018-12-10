from os_dbnetget.commands.qdb import QDB
from os_dbnetget.utils import binary_stdout


class DefaultCommand(QDB):

    def __init__(self):
        super(DefaultCommand, self).__init__()
        self._client = None
        self._output = None

    def _create_output(self, args):
        output = None
        if args.output is None:
            output = binary_stdout
        else:
            if not hasattr(args, 'output_type'):
                output = open(args.output, 'ab')
            elif args.output_type == 'single':
                output = open(args.output, 'ab')
            elif args.output_type == 'rotate':
                from os_rotatefile import open_file
                output = open_file(args.output, 'w')
        return output

    def _close(self):
        try:
            if self._pool:
                self._pool.close()
            if self._output:
                self._output.close()
        except:
            pass

    def run(self, args):

        self._pool = self._create_client_pool(args)
        self._ouput = self._create_output(args)
        stop = False

        def on_stop(signum, frame):
            # try:
            stop = True
            print('---------------------------')
            pool.close()
            print('---------aaaaaaaaaaaaaa-')
            ouput.close()
            print('-----------dddddddddddddddd')
            # except:
            #     pass
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT):
            signal.signal(sig, on_stop)

        try:
            for line in chain.from_iterable(args.inputs):
                if stop:
                    print('break')
                    break
                line = line.strip()
                status = 'N'
                try:
                    d = docid(line)
                except NotImplementedError:
                    status = 'E'
                if status != 'E':
                    proto = create_protocal('get', d.bytes[16:])
                    p = pool.execute(proto)
                    status = 'N'
                    if p.value:
                        status = 'Y'
                        ouput.write(p.value)
                _logger.info('%s\t%s' % (line, status))
        # except Exception as e:
        #     raise e
        #     _logger.error('Error, %s' % str(e))
        finally:
            try:
                pool.close()
                ouput.close()
            except:
                pass
