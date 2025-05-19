from main import runner, utils
import heapq
from result import Ok
import yaml
from pathlib import Path
import os
import parse
import subprocess as sh
import copy
import time
import pandas as pd
import numpy as np
import collections
STRIP_THRESHOLD = 0.03
SERIALIZATION_LIBRARIES = ["cornflakes-dynamic", "cornflakes1c-dynamic",
                           "capnproto", "flatbuffers", "protobuf"]

class ZccCdnIteration(runner.Iteration):
    def __init__(self,
            client_rates,
            key_size,
            trace_file,
            max_num_lines,
            num_threads,
            extra_zcc_params: runner.ExtraZccParameters,
            log_keys = False,
            log_pinning_map = False,
            max_bucket = 16384,
            trial = None):
        self.client_rates = client_rates
        self.key_size = key_size
        self.max_num_lines = max_num_lines
        self.trace_file = trace_file
        self.num_threads = num_threads
        self.extra_zcc_params = extra_zcc_params
        self.system = extra_zcc_params.system
        self.log_keys = log_keys
        self.log_pinning_map = log_pinning_map
        self.max_bucket = max_bucket
        self.trial = trial
    def __str__(self):
        return f"system: {self.system}, " \
                f"trace_file: {self.trace_file}, " \
                f"max_num_lines: {self.max_num_lines}" \
                f"key size: {self.key_size}," \
                f"num_threads: {self.num_threads}, "\
                f"extra zcc_params: {str(self.extra_zcc_params)}, "\
                f"trial: {self.trial}"

    def hash(self):
        args = [self.max_num_lines, self.key_size, self.trace_file,
                self.system,
                str(self.extra_zcc_params), self.num_threads,
                self.trial]
        return args
    def get_iteration_params(self):
        params = ["system", "max_num_lines", "trace_file",  "key_size", "num_threads", "num_clients", "offered_load_pps"]
        params.extend(self.extra_zcc_params.get_iteration_params())
        return params
        
    def get_iteration_params_values(self):
        offered_load_pps = 0
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            offered_load_pps += rate * num * self.num_threads
        ret = {
                "trace_file": self.trace_file,
                "max_num_lines": self.max_num_lines,
                "key_size": self.key_size,
                "offered_load_pps": offered_load_pps,
                "num_threads": self.num_threads,
                "num_clients": self.get_num_clients(),
                "system": self.system,
            }
        ret.update(self.extra_zcc_params.get_iteration_params_values())
        return ret
    
    def get_num_clients(self):
        total_hosts = 0
        for i in self.client_rates:
            total_hosts += i[1]
        return total_hosts
    
    def get_iteration_clients(self, possible_hosts):
        total_hosts = 0
        for i in self.client_rates:
            total_hosts += i[1]
        return possible_hosts[0:total_hosts]

    def find_rate(self, client_options, host):
        rates = []
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            for idx in range(num):
                rates.append(rate)
        try:
            rate_idx = client_options.index(host)
            return rates[rate_idx]
        except:
            utils.error("Host {} not found in client options {}.".format(
                        host,
                        client_options))
            exit(1)
    
    def get_max_num_lines_string(self):
        return "max_num_lines_{}".format(self.max_num_lines)
    def get_key_size_string(self):
        return "key_size_{}".format(self.key_size)
    def get_num_threads(self):
        return self.num_threads
    
    def get_trial(self):
        return self.trial

    def set_trial(self, trial):
        self.trial = trial

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

    def get_num_threads_string(self):
        return "{}_threads".format(self.num_threads)

    def get_client_rate_string(self):
        # 2@300000,1@100000 implies 2 clients at 300000 pkts / sec and 1 at
        # 100000 pkts / sec
        ret = ""
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            if ret != "":
                ret += ","
            ret += "{}@{}".format(num, rate)
        return ret

    def get_parent_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.system /\
                self.extra_zcc_params.get_subfolder() /\
                self.get_max_num_lines_string() /\
                self.get_key_size_string() /\
                self.get_client_rate_string() /\
            self.get_num_threads_string()

    def get_folder_name(self, high_level_folder):
        return self.get_parent_folder(high_level_folder) / self.get_trial_string()

    def get_program_args(self,
                         host,
                         config_yaml,
                         program,
                         programs_metadata):
        ret = {}
        ret["key_size"] = self.key_size
        ret["max_num_lines"] = self.max_num_lines
        ret["trace_file"] = self.trace_file
        ret["library"] = "cornflakes-dynamic"
        ret["client_library"] = "cornflakes-dynamic"
        folder_name = self.get_folder_name(config_yaml["hosts"][host]["tmp_folder"])
        self.extra_zcc_params.fill_in_args(ret, program)
        host_type_map = config_yaml["host_types"]
        server_host = host_type_map["server"][0]
        if program == "start_server":
            ret["server_ip"] = config_yaml["hosts"][host]["ip"]
            ret["mode"] = "server"
            ret["log_keys"] = ""
            ret["log_pinning_map"] = ""
            if self.log_keys:
                # must match what is in experiment yaml
                ret["log_keys"] = f" --record_key_mappings {folder_name}/{host}_keymappings.log"
            if self.log_pinning_map:
                ret["log_pinning_map"] = f" --record_pinning_map {folder_name}/{host}_pinning.log"
        elif program == "start_client":
            host_options = self.get_iteration_clients(
                    host_type_map["client"])
            ret["mode"] = "client"
            rate = self.find_rate(host_options, host)
            ret["num_threads"] = self.num_threads
            ret["num_clients"] = len(host_options)
            ret["num_machines"] = self.get_num_clients()
            ret["machine_id"] = self.find_client_id(host_options, host)
            ret["rate"] = rate

            # calculate server host
            ret["server_ip"] =  config_yaml["hosts"][server_host]["ip"]
            ret["client_ip"] = config_yaml["hosts"][host]["ip"]
        else:
            utils.error("Unknown program name: {}".format(program))
            exit(1)
        return ret
    
    
    def calculate_iteration_stats(self, local_folder, client_file_list,
            print_stats):
        """
        Analyzes this iteration stats.
        If print_stats is true, prints out values of parameters and values
        of stats.
        Default implementation assumes Rust clients that implement the state
        machine trait.
        Otherwise, writes csv into local_folder/analysis.log.
        Overwritten as offered load pps or calculating gbps doesn't apply for
        twitter trace.
        """
        # TODO: add in offered load stats
        packets_received = 0
        packets_sent = 0
        max_runtime = 0
        size_bucket_histogram = {}
        histogram = utils.Histogram({})
        for filename in client_file_list:
            # format:
            # map 1 = total histogram over all sizes
            # map 2 = per size histogram (summed over these threads)
            # map 3 = int -> thread stats
            yaml_map = yaml.load(Path(filename).read_text(), Loader =
                    yaml.FullLoader)
            total_histogram = utils.Histogram(yaml_map[0])
            histogram.combine(total_histogram)
            for size_bucket_str in yaml_map[1]:
                size_bucket = int(size_bucket_str)
                hist = utils.Histogram(yaml_map[1][size_bucket_str])
                if size_bucket not in size_bucket_histogram:
                    size_bucket_histogram[size_bucket] = hist
                else:
                    size_bucket_histogram[size_bucket].combine(hist)

            threads_map = yaml_map[2]
            for thread, thread_info in threads_map.items():
                packets_sent += thread_info["num_sent"]
                packets_received += thread_info["num_received"]
                thread_runtime = thread_info["runtime"]
                max_runtime = max(thread_runtime, max_runtime)

        # calculate full statistics
        achieved_load_pps = float(packets_received) / max_runtime
        achieved_load_pps_sent = float(packets_sent) / max_runtime
        iteration_params = self.get_iteration_params_values()
        total_count = total_histogram.count

        percent_achieved = float(achieved_load_pps) / float(achieved_load_pps_sent)
        iteration_params["achieved_load_pps"] = achieved_load_pps
        iteration_params["achieved_load_pps_sent"] = achieved_load_pps_sent
        iteration_params["percent_achieved_rate"] = percent_achieved
        iteration_params["avg"] = total_histogram.avg() / float(1000)
        iteration_params["median"] = total_histogram.value_at_quantile(0.50) / float(1000)
        iteration_params["p75"] = histogram.value_at_quantile(0.75) / float(1000)
        iteration_params["p80"] = histogram.value_at_quantile(0.80) / float(1000)
        iteration_params["p85"] = histogram.value_at_quantile(0.85) / float(1000)
        iteration_params["p90"] = histogram.value_at_quantile(0.90) / float(1000)
        iteration_params["p95"] = histogram.value_at_quantile(0.95) / float(1000)
        iteration_params["p99"] = histogram.value_at_quantile(0.99) / float(1000)
        iteration_params["p999"] = histogram.value_at_quantile(0.999) / float(1000)

        format_string_params = ["{{{}}}".format(x) for x in
                self.get_csv_header()]
        format_string = ",".join(format_string_params)
        self.fill_in_buckets(iteration_params, size_bucket_histogram)
        format_string = format_string.format(**iteration_params)
        analysis_path = Path(local_folder) / "analysis.log"
        with open(str(analysis_path), "w") as f:
            f.write(",".join(self.get_csv_header()) + os.linesep)
            f.write(format_string + os.linesep)
            f.close()
        if print_stats:
            print_string = "Experiment results: \n"\
                               "\t- achieved load received: {:.4f} req/s\n"\
                               "\t- achieved load sent: {:.4f} req/s\n"\
                               "\t- percentage achieved rate: {:.4f}\n"\
                               "\t- avg latency: {:.4f}"\
                               " \u03BCs\n\t- median: {:"\
                               ".4f} \u03BCs\n\t- p99: {: .4f}"\
                               " \u03BCs\n\t- p999:"\
                               "{: .4f} \u03BCs".format(
                                       achieved_load_pps,
                                       achieved_load_pps_sent,
                                   percent_achieved,
                                   iteration_params["avg"],
                                   iteration_params["median"],
                                   iteration_params["p99"],
                                   iteration_params["p999"])
            min_bucket = 8
            while min_bucket <= self.max_bucket:
                if min_bucket not in size_bucket_histogram:
                    min_bucket *= 2
                    continue
                print_string += "\n\t- Size {:d}: median: {: .4f} \u03BCs".format(min_bucket, iteration_params["size{}_p50".format(min_bucket)])
                print_string += " & p99: {: .4f} \u03BCs ({:d} reqs or {: .2f} %)".format(iteration_params["size{}_p99".format(min_bucket)],
                        iteration_params["size{}_count".format(min_bucket)],
                        float(iteration_params["size{}_count".format(min_bucket)])
                        / float(total_count))
                min_bucket *= 2
            utils.info(print_string)


    
    def get_bucket_list(self):
        ret = []
        bucket_size = 8
        while bucket_size <= self.max_bucket:
            ret.extend(["size{}_p50".format(bucket_size),
                "size{}_p99".format(bucket_size),
                "size{}_count".format(bucket_size)])
            bucket_size *= 2
        return ret

    def fill_in_buckets(self, iteration_params, size_bucket_histogram):
        bucket_size = 8
        while bucket_size <= self.max_bucket:
            if bucket_size in size_bucket_histogram:
                hist = size_bucket_histogram[bucket_size]
                iteration_params["size{}_p50".format(bucket_size)] = hist.value_at_quantile(0.50) / float(1000)
                iteration_params["size{}_p99".format(bucket_size)] = hist.value_at_quantile(0.99) / float(1000)
                iteration_params["size{}_count".format(bucket_size)] = hist.count
            else:
                iteration_params["size{}_p50".format(bucket_size)] = 0
                iteration_params["size{}_p99".format(bucket_size)] = 0
                iteration_params["size{}_count".format(bucket_size)] = 0
            bucket_size *= 2

    def get_csv_header(self):
        csv_order = self.get_iteration_params()
        csv_order.extend(["achieved_load_pps", "achieved_load_pps_sent",
        "percent_achieved_rate", "avg", "median", "p75", "p80", "p85",
        "p90", "p95", "p99", "p999"])
        csv_order.extend(self.get_bucket_list())
        return csv_order

# for each system -- we can vary the following parameters
ZccCdnExpInfo = collections.namedtuple("ZCCKVExpInfo", 
        ["register_at_start",
        "pinning_limit",
        "segment_size", 
        "pinning_frequency"])

class ZccCdnBench(runner.Experiment):
    def __init__(self, exp_yaml, config_yaml):
        self.exp = "ZccCdnBench"
        self.exp_yaml = yaml.load(Path(exp_yaml).read_text(),
                Loader=yaml.FullLoader)
        self.config_yaml = yaml.load(Path(config_yaml).read_text(),
                Loader=yaml.FullLoader)

    def experiment_name(self):
        return self.exp

    def skip_iteration(self, total_args, iteration):
        return False

    def append_to_skip_info(self, total_args, iteration, higher_level_folder):
        return

    def parse_exp_info_string(self, exp_string):
        """
        Returns parsed ZccCdnExpInfo from exp_string.
        Should be formatted as:
        register_at_start = {1|0}, pinning_limit = {}, segment_size = {}, pinning_frequency = {}
        max_num_lines = {}, key_size = {}, 
        """
        try:
            parse_result = parse.parse("register_at_start = {:d}, pinning_limit = {:d}, segment_size = {}, pinning_frequency = {}", exp_string)
            register_at_start = True
            if parse_result[0] == 0:
                register_at_start = False
            elif parse_result[0] == 1:
                register_at_start = True
            else:
                utils.warn("Must pass in 1|0 for register_at_start")
                exit(1)
            return ZccCdnExpInfo(register_at_start, parse_result[1],
                    parse_result[2], parse_result[3])
        except:
            utils.error("Error parsing exp_string: {}".format(exp_string))
            exit(1)


    def get_iterations(self, total_args):
        if total_args.exp_type == "individual":
            if total_args.num_clients > int(self.config_yaml["max_clients"]):
                utils.error("Cannot have {} clients, greater than max {}"
                            .format(total_args.num_clients,
                                    self.config_yaml["max_clients"]))
                exit(1)
            extra_zcc_params = runner.ExtraZccParameters(total_args.system,
                    zcc_pinning_limit=total_args.zcc_pinning_budget,
                    zcc_segment_size=total_args.zcc_segment_size,
                    register_at_start=total_args.zcc_register_at_start,
                    zcc_pinning_frequency=total_args.zcc_pinning_frequency)
            client_rates = [(total_args.rate, total_args.num_clients)]
            it = ZccCdnIteration(
                    client_rates,
                    total_args.key_size,
                    total_args.trace_file,
                    total_args.max_num_lines,
                    total_args.num_threads,
                    extra_zcc_params,
                    total_args.log_keys,
                    total_args.log_pinning_map,
                    trial = None)
            num_trials_finished = utils.parse_number_trials_done(
                it.get_parent_folder(total_args.folder))
            if total_args.analysis_only or total_args.graph_only:
                ret = []
                for i in range(0, num_trials_finished):
                    it_clone = copy.deepcopy(it)
                    it_clone.set_trial(i)
                    ret.append(it_clone)
                return ret
            it.set_trial(num_trials_finished)
            return [it]
        else:
            ret = []
            loop_yaml = self.get_loop_yaml()
            # loop over various options
            num_trials = utils.yaml_get(loop_yaml, "num_trials")
            num_threads = utils.yaml_get(loop_yaml, "num_threads")
            num_clients = utils.yaml_get(loop_yaml, "num_clients")
            rate_percentages = utils.yaml_get(loop_yaml, "rate_percentages")
            key_size = utils.yaml_get(loop_yaml, "key_size")
            max_num_lines = utils.yaml_get(loop_yaml, "max_num_lines")
            systems = utils.yaml_get(loop_yaml, "systems")
            num_pages_per_mempool = utils.yaml_get(loop_yaml, "num_pages_per_mempool")
            max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))
            for trial in range(num_trials):
                for system in systems:
                    for rate_percentage in rate_percentages:
                        for zcckvexp in max_rates_dict:
                            max_rate = max_rates_dict[zcckvexp]
                            rate = int(float(max_rate) * rate_percentage)
                            client_rates = [(rate, num_clients)]
                            extra_zcc_params = runner.ExtraZccParameters(system,
                                    zcc_pinning_limit=zcckvexp.pinning_limit,
                                    zcc_segment_size=zcckvexp.segment_size,
                                    register_at_start=zcckvexp.register_at_start,
                                    zcc_pinning_frequency=zcckvexp.pinning_frequency,
                                    num_pages_per_mempool=num_pages_per_mempool)
                            it = ZccCdnIteration(
                                    client_rates,
                                    key_size,
                                    total_args.trace_file,
                                    max_num_lines,
                                    num_threads,
                                    extra_zcc_params,
                                    total_args.log_keys,
                                    total_args.log_pinning_map,
                                    trial = trial)
                            ret.append(it)
            return ret
    
    def add_specific_args(self, parser, namespace):
        parser.add_argument("-l", "--logfile",
                            help="logfile name",
                            default="latencies.log")
        parser.add_argument("-mnl", "--max_num_lines",
                            dest="max_num_lines",
                            type = int,
                            default = 1000000)
        parser.add_argument("-tr", "--trace_file",
                            dest = "trace_file")
        parser.add_argument("-ks", "--key_size",
                            dest="key_size",
                            type = int,
                            default = 32,
                            )
        parser.add_argument("--log_keys",
                            dest="log_keys",
                            action="store_true",
                            help = "Whether to log key mappings")
        parser.add_argument("--log_pinning_map",
                            dest="log_pinning_map",
                            action="store_true",
                            help = "Whether to log the pinning map at each epoch.")
        if namespace.exp_type == "individual":
            parser.add_argument("-nt", "--num_threads",
                                dest="num_threads",
                                type=int,
                                default=1,
                                help="Number of threads to run with")
            parser.add_argument("-r", "--rate",
                                dest="rate",
                                type=int,
                                default=1.0,
                                help="Offered load per thread client")
            parser.add_argument("-nc", "--num_clients",
                                dest="num_clients",
                                type=int,
                                default=1)
            parser.add_argument("-dist", "--distribution",
                                dest="distribution",
                                choices=["exponential", "uniform"],
                                default = "uniform")
            parser = runner.extend_with_zcc_parameters(parser) 
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def exp_post_process_analysis(self, total_args, logfile, new_logfile):
        pass

    def graph_results(self, args, folder, logfile, post_process_logfile):
        pass


def main():
    parser, namespace = runner.get_basic_args()
    google_bench = ZccCdnBench(
        namespace.exp_config,
        namespace.config)
    google_bench.execute(parser, namespace)

if __name__ == '__main__':
    main()

