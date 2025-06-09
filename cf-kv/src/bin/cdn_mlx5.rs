use cf_kv::{
    capnproto::{CapnprotoClient, CapnprotoKVServer},
    cdn::{CdnClient, CdnServerLoader},
    cornflakes_dynamic::{CornflakesClient, CornflakesKVServer},
    flatbuffers::{FlatbuffersClient, FlatbuffersKVServer},
    init_zcc_logging, init_zcc_pin_and_unpin_thread,
    protobuf::{ProtobufClient, ProtobufKVServer},
    run_cdn::*,
    run_client_cdn, run_mlx5_cornflakes_with_zcc, run_server_cdn, set_zcc_and_mempool_parameters,
    KVClient,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use mlx5_datapath::datapath::connection::{CornflakesMlx5Slab, Mlx5Connection};
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = CdnOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_mlx5_cornflakes_with_zcc!(opt, run_server_cdn);
            }
            SerializationType::Flatbuffers => {
                run_server_cdn!(
                    FlatbuffersKVServer<
                        Mlx5Connection<
                            zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                        >,
                    >,
                    Mlx5Connection<
                        zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                    >,
                    opt
                );
            }
            SerializationType::Capnproto => {
                run_server_cdn!(
                    CapnprotoKVServer<
                        Mlx5Connection<
                            zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                        >,
                    >,
                    Mlx5Connection<
                        zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                    >,
                    opt
                );
            }
            SerializationType::Protobuf => {
                run_server_cdn!(
                    ProtobufKVServer<
                        Mlx5Connection<
                            zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                        >,
                    >,
                    Mlx5Connection<
                        zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                    >,
                    opt
                );
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client_cdn!(
                    CornflakesClient<
                        Mlx5Connection<
                            zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                        >,
                    >,
                    Mlx5Connection<
                        zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                    >,
                    opt
                );
            }
            SerializationType::Flatbuffers => {
                run_client_cdn!(
                    FlatbuffersClient<
                        Mlx5Connection<
                            zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                        >,
                    >,
                    Mlx5Connection<
                        zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                    >,
                    opt
                );
            }
            SerializationType::Capnproto => {
                run_client_cdn!(
                    CapnprotoClient<
                        Mlx5Connection<
                            zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                        >,
                    >,
                    Mlx5Connection<
                        zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                    >,
                    opt
                );
            }
            SerializationType::Protobuf => {
                run_client_cdn!(
                    ProtobufClient<
                        Mlx5Connection<
                            zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                        >,
                    >,
                    Mlx5Connection<
                        zero_copy_cache::data_structures::NoAlgCache<CornflakesMlx5Slab>,
                    >,
                    opt
                );
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
