#!/usr/bin/env python3
"""
Test: Composite two MP4 videos (main + overlay) with looping
Main video plays once, overlay loops continuously using imagefreeze workaround
"""

import sys
import subprocess
import tempfile
import os

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

Gst.init(None)


class VideoCompositor:
    """Composite two videos: main video with looping PIP overlay"""

    def __init__(self, main_url, overlay_url, overlay_x=640, overlay_y=360, overlay_width=320, overlay_height=180):
        self.main_url = main_url
        self.overlay_url = overlay_url
        self.overlay_x = overlay_x
        self.overlay_y = overlay_y
        self.overlay_width = overlay_width
        self.overlay_height = overlay_height
        self.pipeline = None
        self.looped_overlay_file = None

    def _create_looped_overlay(self, loop_count=100):
        """Pre-create a looped version of the overlay video using FFmpeg"""
        print(f"[SETUP] Creating looped overlay video ({loop_count} loops)...")

        # Create temp file
        self.looped_overlay_file = tempfile.mktemp(suffix=".mp4")

        # Use FFmpeg to loop the video
        cmd = [
            "ffmpeg", "-y",
            "-stream_loop", str(loop_count - 1),
            "-i", self.overlay_url,
            "-c", "copy",
            "-t", "3600",  # Max 1 hour
            self.looped_overlay_file
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, timeout=60)
            if result.returncode == 0:
                print(f"[SETUP] Looped overlay created: {self.looped_overlay_file}")
                return True
            else:
                print(f"[SETUP] FFmpeg failed: {result.stderr.decode()[:200]}")
                return False
        except Exception as e:
            print(f"[SETUP] FFmpeg error: {e}")
            return False

    def _create_pipeline(self):
        """Create compositing pipeline"""

        # Use the looped file if available, otherwise use original
        overlay_uri = f"file://{self.looped_overlay_file}" if self.looped_overlay_file else self.overlay_url

        pipeline_str = f"""
            compositor name=comp
                sink_0::zorder=0
                sink_1::zorder=1
                sink_1::xpos={self.overlay_x}
                sink_1::ypos={self.overlay_y}
                sink_1::width={self.overlay_width}
                sink_1::height={self.overlay_height} !
            videoconvert !
            videoscale !
            video/x-raw,width=1280,height=720 !
            autovideosink

            uridecodebin uri="{self.main_url}" name=main_src !
            queue max-size-buffers=5 !
            videoconvert !
            videoscale !
            video/x-raw,width=1280,height=720 !
            comp.sink_0

            main_src. !
            queue max-size-buffers=5 !
            audioconvert !
            audioresample !
            autoaudiosink

            uridecodebin uri="{overlay_uri}" name=overlay_src !
            queue max-size-buffers=5 !
            videoconvert !
            videoscale !
            video/x-raw,width={self.overlay_width},height={self.overlay_height} !
            comp.sink_1
        """

        try:
            self.pipeline = Gst.parse_launch(pipeline_str)

            # Setup bus
            bus = self.pipeline.get_bus()
            bus.add_signal_watch()
            bus.connect("message", self._on_message)

            return True
        except GLib.Error as e:
            print(f"[ERROR] Pipeline failed: {e}")
            return False

    def _on_message(self, bus, msg):
        if msg.type == Gst.MessageType.EOS:
            print("[MAIN] Stream ended, looping...")
            self.pipeline.seek_simple(
                Gst.Format.TIME,
                Gst.SeekFlags.FLUSH | Gst.SeekFlags.KEY_UNIT,
                0
            )

        elif msg.type == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            src_name = msg.src.get_name() if msg.src else "unknown"
            print(f"[ERROR] {src_name}: {err}")
            print(f"[DEBUG] {debug}")
            Gtk.main_quit()

        elif msg.type == Gst.MessageType.STATE_CHANGED:
            if msg.src == self.pipeline:
                old, new, _ = msg.parse_state_changed()
                if new == Gst.State.PLAYING:
                    print("[STATE] Pipeline playing")

        return True

    def run(self):
        Gtk.init(None)

        print("[COMPOSITOR] Video Compositor with Looping PIP Overlay")
        print(f"  Main: {self.main_url[:60]}...")
        print(f"  Overlay: {self.overlay_url[:60]}...")
        print(f"  Overlay position: ({self.overlay_x}, {self.overlay_y})")
        print(f"  Overlay size: {self.overlay_width}x{self.overlay_height}")

        # Create looped overlay file
        if not self._create_looped_overlay():
            print("[WARNING] Using original overlay (may not loop)")

        if not self._create_pipeline():
            return

        print("\n[COMPOSITOR] Starting playback...")
        self.pipeline.set_state(Gst.State.PLAYING)

        print("Press Ctrl+C to stop\n")

        try:
            Gtk.main()
        except KeyboardInterrupt:
            print("\nStopping...")
        finally:
            self.pipeline.set_state(Gst.State.NULL)
            # Cleanup temp file
            if self.looped_overlay_file and os.path.exists(self.looped_overlay_file):
                os.remove(self.looped_overlay_file)
                print(f"[CLEANUP] Removed temp file")

        print("Done.")


def main():
    # Main video and overlay video
    main_url = "https://storage.googleapis.com/gvabox/media/samples/stock.mp4"
    overlay_url = "https://www.w3schools.com/html/mov_bbb.mp4"

    # Create compositor with overlay in bottom-right corner
    compositor = VideoCompositor(
        main_url=main_url,
        overlay_url=overlay_url,
        overlay_x=920,      # Bottom-right X
        overlay_y=500,      # Bottom-right Y
        overlay_width=320,
        overlay_height=180
    )
    compositor.run()


if __name__ == "__main__":
    main()
