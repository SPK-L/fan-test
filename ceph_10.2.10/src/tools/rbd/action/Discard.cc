// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/Context.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace discard {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_discard(librbd::Image& image, uint64_t offset, uint64_t len,
                      bool no_progress)
{
  int r;
  uint64_t img_size = 0;
  utils::ProgressContext pc("Discarding image", no_progress);

  r = image.size(&img_size);
  if (r < 0) {
    cerr << "rbd: failed to get image size" << std::endl;
    goto done;
  }

  if (offset > img_size ) {
    r = -EFAULT;
    cerr << "rbd: offset overflow image size" << std::endl;
    goto done;
  }
  if ((offset + len) > img_size ) {
    r = -EFAULT;
    cerr << "rbd: offset+len  overflow image size,please check" << std::endl;
    goto done;
  }

  r = image.discard(offset, len);

done:
  if(r < 0)
    pc.fail();
  else
    pc.finish();

  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_extent_options(options);
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  /*
  std::string path;
  r = utils::get_path(vm, utils::get_positional_argument(vm, 1), &path);
  if (r < 0) {
    return r;
  }
  */

  uint64_t offset, len;
  r = utils::get_extent_options(vm, &offset, &len);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_discard(image, offset, len, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: discard error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"discard"}, {}, "discard special place of the image.", "",
  &get_arguments, &execute);

} // namespace discard
} // namespace action
} // namespace rbd
