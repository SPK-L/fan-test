// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/Context.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "common/ceph_crypto.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace read {

namespace at = argument_types;
namespace po = boost::program_options;

using ceph::crypto::MD5;

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

struct ReadContext {
  librbd::Image *image;
  int fd;
  uint64_t totalsize;
  utils::ProgressContext pc;
  OrderedThrottle throttle;
  MD5 *hash;
  bool checksum;

  ReadContext(librbd::Image *i, int f, uint64_t t, int max_ops,
              bool no_progress, MD5 *hash, bool checksum) :
    image(i), fd(f), totalsize(t), pc("Reading image", no_progress),
    throttle(max_ops, true), hash(hash), checksum(checksum) {
  }
};

class C_Read : public Context
{
public:
  C_Read(ReadContext *rdc, uint64_t offset, uint64_t pos, uint64_t length)
    : m_read_context(rdc), m_offset(offset), m_pos(pos), m_length(length)
  {
    m_fd = m_read_context->fd;
    m_hash = m_read_context->hash;
    m_checksum = m_read_context->checksum;
  }

  int send()
  {
    if (m_read_context->throttle.pending_error()) {
      return m_read_context->throttle.wait_for_ret();
    }

    C_OrderedThrottle *ctx = m_read_context->throttle.start_op(this);
    int op_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                   LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
    librbd::RBD::AioCompletion *aio_completion =
      new librbd::RBD::AioCompletion(ctx, &utils::aio_context_callback);
    int r = m_read_context->image->aio_read2(m_offset, m_length, m_read_data,
                                             aio_completion, op_flags);
    if (r < 0) {
      cerr << "rbd: error requesting read from source image" << std::endl;
      aio_completion->release();
      ctx->complete(r);
    }

    return 0;
  }

protected:
  virtual void finish(int r)
  {
    if (r < 0) {
      cerr << "rbd: error reading from source image at offset "
           << m_offset << ": " << cpp_strerror(r) << std::endl;
    }

    if (r >= 0) {
      assert(m_read_data.length() == static_cast<size_t>(r));
      if (m_checksum) {
        unsigned in_len = m_read_data.length();
        m_hash->Update((const byte *)m_read_data.c_str(), in_len);
      }

      if (m_fd != STDOUT_FILENO) {
        if (m_read_data.is_zero()) {
          goto done;
        }

        uint64_t chkret = lseek64(m_fd, m_pos, SEEK_SET);
        if (chkret != m_pos) {
          cerr << "rbd: error seeking destination image to offset "
            << m_pos << std::endl;
          r = -errno;
          goto done;
        }
      }

      r = m_read_data.write_fd(m_fd);
      if (r < 0) {
        cerr << "rbd: error writing to destination image at offset "
          << m_offset << std::endl;
      }
    }

done:
    m_read_context->pc.update_progress(m_pos, m_read_context->totalsize);
    m_read_context->throttle.end_op(r);
  }

private:
  ReadContext *m_read_context;
  bufferlist m_read_data;
  uint64_t m_offset;
  uint64_t m_pos;
  uint64_t m_length;
  MD5 *m_hash;
  bool m_checksum;
  int m_fd;
};

static int do_read(librbd::Image& image, uint64_t offset, uint64_t len,
                   const char *path, bool checksum, bool no_progress)
{
  int fd, r;
  int max_concurrent_ops;
  uint64_t period;
  utils::ProgressContext pc("Reading image", no_progress);
  MD5 hash;

  librbd::image_info_t info;
  r =  image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  bool to_stdout = (strcmp(path, "-") == 0);
  if (to_stdout) {
    fd = STDOUT_FILENO;
  } else {
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
      return -errno;
    }
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
  }
  max_concurrent_ops = max(g_conf->rbd_concurrent_management_ops, 1);

  ReadContext rdc(&image, fd, len, max_concurrent_ops, no_progress, &hash,
                  checksum);

  if (offset > info.size) {
    r = -EFAULT;
    cerr << "rbd: offset overflow image size" << std::endl;
    goto done;
  }
  if ((offset + len) > info.size) {
    r = -EFAULT;
    cerr << "rbd: offset+len  overflow image size,please check" << std::endl;
    goto done;
  }

  period = image.get_stripe_count() * (1ull << info.order);
  //do read in loop, we read each time use max size 4M
  for (uint64_t pos = 0; pos < len; ) {
    if (rdc.throttle.pending_error()) {
      break;
    }

    uint64_t length = min(period, len - pos);
    C_Read *ctx = new C_Read(&rdc, offset + pos, pos, length);
    ctx->send();

    pos += length;
  }

  r = rdc.throttle.wait_for_ret();
  if (checksum) {
    char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char final_etag_hex[CEPH_CRYPTO_MD5_DIGESTSIZE * 2];
    hash.Final((byte *)final_etag);
    buf_to_hex((const unsigned char *)final_etag, CEPH_CRYPTO_MD5_DIGESTSIZE,
               final_etag_hex);
    bufferlist ptr;
    ptr.append((const char *)final_etag_hex, CEPH_CRYPTO_MD5_DIGESTSIZE * 2);
    r = ptr.write_fd(fd);
    if (r < 0) {
      cerr << "rbd: error writing checksum to fd " << std::endl;
      goto done;
    }
  }

done:
  if (!to_stdout) {
    if (r >= 0) {
      if (checksum) {
        len += (CEPH_CRYPTO_MD5_DIGESTSIZE * 2);
      }
      r = ftruncate(fd, len);
    }
    close(fd);
  }

  if (r < 0) {
    pc.fail();
  } else {
    pc.finish();
  }

  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  // make path-name at last
  at::add_path_options(positional, options, "write file (or '-' for stdout)");
  at::add_extent_options(options);
  at::add_checksum_option(options);
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

  std::string path;
  r = utils::get_path(vm, utils::get_positional_argument(vm, 1), &path);
  if (r < 0) {
    return r;
  }

  uint64_t offset, len;
  bool checksum = vm["checksum"].as<bool>();
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

  r = do_read(image, offset, len, path.c_str(), checksum,
              vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: read error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::SwitchArguments switched_arguments({"checksum"});
Shell::Action action(
  {"read"}, {}, "read special place of the image.", "",
  &get_arguments, &execute);

} // namespace read
} // namespace action
} // namespace rbd
