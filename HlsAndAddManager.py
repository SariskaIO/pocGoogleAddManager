#!/usr/bin/env python3
"""
HLS + Looping MP4 Ad Overlay using GStreamer
Uses playbin for ad with proper looping via seek on EOS
"""

import sys

# macOS: Initialize Cocoa BEFORE importing GStreamer
if sys.platform == 'darwin':
    try:
        import AppKit
        AppKit.NSApplication.sharedApplication()
    except ImportError:
        print("Note: PyObjC not installed")

import gi
gi.require_version('Gst', '1.0')
gi.require_version('Gtk', '3.0')
from gi.repository import Gst, Gtk, GLib

# Initialize GStreamer
Gst.init(None)


class HLSWithOverlay:
    """Play HLS with looping MP4 overlay using separate pipelines"""

    def __init__(self, main_url, ad_url, rtmp_url=None, ad_x=920, ad_y=500, ad_width=320, ad_height=180):
        self.main_url = main_url
        self.ad_url = ad_url
        self.rtmp_url = rtmp_url
        self.ad_x = ad_x
        self.ad_y = ad_y
        self.ad_width = ad_width
        self.ad_height = ad_height

        self.pipeline = None
        self.ad_playbin = None
        self.appsrc = None
        self.running = True

    def _create_main_pipeline(self):
        """Create main pipeline with appsrc for ad overlay"""

        if self.rtmp_url:
            video_output = f"""
                queue !
                x264enc tune=zerolatency bitrate=2500 speed-preset=superfast !
                video/x-h264,profile=baseline !
                flvmux name=mux streamable=true !
                rtmpsink location="{self.rtmp_url} live=1"
            """
            audio_output = "queue ! voaacenc bitrate=128000 ! mux."
        else:
            video_output = "videoconvert ! autovideosink"
            audio_output = "autoaudiosink"

        pipeline_str = f"""
            compositor name=comp
                sink_0::zorder=0
                sink_1::zorder=1
                sink_1::xpos={self.ad_x}
                sink_1::ypos={self.ad_y}
                sink_1::width={self.ad_width}
                sink_1::height={self.ad_height} !
            videoconvert !
            videoscale !
            video/x-raw,width=1280,height=720 !
            {video_output}

            uridecodebin uri="{self.main_url}" name=main_src !
            queue max-size-buffers=5 !
            videoconvert !
            videoscale !
            comp.sink_0

            main_src. !
            queue max-size-buffers=5 !
            audioconvert !
            audioresample !
            {audio_output}

            appsrc name=ad_appsrc is-live=true format=time !
            queue max-size-buffers=5 leaky=downstream !
            videoconvert !
            videoscale !
            video/x-raw,width={self.ad_width},height={self.ad_height} !
            comp.sink_1
        """

        try:
            self.pipeline = Gst.parse_launch(pipeline_str)
            self.appsrc = self.pipeline.get_by_name("ad_appsrc")

            # Set caps on appsrc
            caps = Gst.Caps.from_string(
                f"video/x-raw,format=I420,width={self.ad_width},height={self.ad_height},framerate=30/1"
            )
            self.appsrc.set_property("caps", caps)

            return True
        except GLib.Error as e:
            print(f"[ERROR] Main pipeline failed: {e}")
            return False

    def _create_ad_playbin(self):
        """Create playbin for ad that feeds into appsrc"""
        self.ad_playbin = Gst.ElementFactory.make("playbin", "ad_playbin")
        self.ad_playbin.set_property("uri", self.ad_url)
        self.ad_playbin.set_property("volume", 0.0)

        # Create custom video sink
        sink_bin = Gst.Bin.new("ad_sink_bin")

        convert = Gst.ElementFactory.make("videoconvert", "ad_convert")
        scale = Gst.ElementFactory.make("videoscale", "ad_scale")
        capsfilter = Gst.ElementFactory.make("capsfilter", "ad_caps")
        capsfilter.set_property("caps", Gst.Caps.from_string(
            f"video/x-raw,format=I420,width={self.ad_width},height={self.ad_height}"
        ))

        appsink = Gst.ElementFactory.make("appsink", "ad_appsink")
        appsink.set_property("emit-signals", True)
        appsink.set_property("sync", True)  # Sync to clock for proper timing
        appsink.set_property("max-buffers", 5)
        appsink.set_property("drop", False)

        for elem in [convert, scale, capsfilter, appsink]:
            sink_bin.add(elem)

        convert.link(scale)
        scale.link(capsfilter)
        capsfilter.link(appsink)

        ghost = Gst.GhostPad.new("sink", convert.get_static_pad("sink"))
        sink_bin.add_pad(ghost)

        self.ad_playbin.set_property("video-sink", sink_bin)

        # Connect sample handler
        def on_new_sample(sink):
            if not self.running or not self.appsrc:
                return Gst.FlowReturn.OK

            sample = sink.emit("pull-sample")
            if sample:
                buf = sample.get_buffer()
                if buf:
                    self.appsrc.emit("push-buffer", buf)
            return Gst.FlowReturn.OK

        appsink.connect("new-sample", on_new_sample)

        # Handle EOS for looping
        bus = self.ad_playbin.get_bus()
        bus.add_signal_watch()

        def on_ad_message(bus, msg):
            if msg.type == Gst.MessageType.EOS:
                print("[AD] Looping...")
                # Seek back to start
                self.ad_playbin.seek_simple(
                    Gst.Format.TIME,
                    Gst.SeekFlags.FLUSH | Gst.SeekFlags.KEY_UNIT,
                    0
                )
            elif msg.type == Gst.MessageType.ERROR:
                err, _ = msg.parse_error()
                print(f"[AD ERROR] {err}")
            return True

        bus.connect("message", on_ad_message)

    def _on_message(self, bus, message):
        t = message.type

        if t == Gst.MessageType.EOS:
            print("[MAIN] End of stream")
            Gtk.main_quit()
        elif t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            src = message.src.get_name() if message.src else ""
            print(f"[ERROR] {src}: {err}")
            Gtk.main_quit()
        elif t == Gst.MessageType.STATE_CHANGED:
            if message.src == self.pipeline:
                old, new, _ = message.parse_state_changed()
                if new == Gst.State.PLAYING:
                    print("[STATE] Main pipeline playing")

        return True

    def run(self):
        Gtk.init(None)

        print("[PLAYER] Starting...")
        print(f"  HLS: {self.main_url[:60]}...")
        print(f"  Ad: {self.ad_url[:60]}...")
        if self.rtmp_url:
            print(f"  RTMP: {self.rtmp_url}")
        else:
            print("  Output: Display")

        if not self._create_main_pipeline():
            return

        self._create_ad_playbin()

        # Setup main bus
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_message)

        print("[PLAYER] Starting playback...")

        # Start ad playbin first
        self.ad_playbin.set_state(Gst.State.PLAYING)

        # Small delay then start main
        GLib.timeout_add(200, lambda: self.pipeline.set_state(Gst.State.PLAYING) or False)

        print("Press Ctrl+C to stop\n")

        try:
            Gtk.main()
        except KeyboardInterrupt:
            print("\nStopping...")
        finally:
            self.running = False
            self.ad_playbin.set_state(Gst.State.NULL)
            self.pipeline.set_state(Gst.State.NULL)

        print("Done.")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="HLS Stream with Ad Overlay - outputs to RTMP or display",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Display locally (for testing)
  python HlsAndAddManager.py

  # Output to RTMP server
  python HlsAndAddManager.py --rtmp rtmp://your-server/live/stream_key

  # Custom HLS and ad
  python HlsAndAddManager.py --hls "https://example.com/stream.m3u8" --ad "https://example.com/ad.mp4"
        """
    )

    parser.add_argument("--hls", default="https://indiatodaylive.akamaized.net/hls/live/2014320/indiatoday/indiatodaylive/live_720p/chunks.m3u8",
                        help="HLS stream URL")
    parser.add_argument("--ad", default="https://storage.googleapis.com/gvabox/media/samples/stock.mp4",
                        help="Ad video URL")
    parser.add_argument("--rtmp", default=None,
                        help="RTMP output URL (optional)")
    parser.add_argument("--ad-x", type=int, default=920, help="Ad X position")
    parser.add_argument("--ad-y", type=int, default=500, help="Ad Y position")
    parser.add_argument("--ad-width", type=int, default=320, help="Ad width")
    parser.add_argument("--ad-height", type=int, default=180, help="Ad height")

    args = parser.parse_args()

    player = HLSWithOverlay(
        main_url=args.hls,
        ad_url=args.ad,
        rtmp_url=args.rtmp,
        ad_x=args.ad_x,
        ad_y=args.ad_y,
        ad_width=args.ad_width,
        ad_height=args.ad_height
    )

    player.run()


if __name__ == "__main__":
    main()
