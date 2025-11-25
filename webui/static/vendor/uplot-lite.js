(function (global) {
  class UPlotLite {
    constructor(opts, data, root) {
      this.opts = Object.assign({ width: 800, height: 400 }, opts || {});
      this.data = data || [[], []];
      this.root = typeof root === 'string' ? document.querySelector(root) : root;
      this.canvas = document.createElement('canvas');
      this.ctx = this.canvas.getContext('2d');
      this.resize(this.opts.width, this.opts.height);
      this.root.classList.add('uplot');
      this.root.innerHTML = '';
      this.root.appendChild(this.canvas);

      this.legend = document.createElement('div');
      this.legend.className = 'uplot-legend';
      this.legend.textContent = (this.opts.series && this.opts.series[1]?.label) || '';
      this.root.appendChild(this.legend);

      this.setData(this.data, true);
      window.addEventListener('resize', () => {
        if (this.opts.resize) {
          const { width, height } = this.opts.resize();
          this.resize(width, height);
          this.draw();
        }
      });
    }

    resize(width, height) {
      this.canvas.width = width;
      this.canvas.height = height;
      this.width = width;
      this.height = height;
    }

    setData(data, skipDraw) {
      this.data = data;
      if (!skipDraw) {
        this.draw();
      }
    }

    draw() {
      const ctx = this.ctx;
      const { width: w, height: h } = this;
      const [xVals, ...series] = this.data;
      ctx.clearRect(0, 0, w, h);

      ctx.fillStyle = this.opts.background || '#000';
      ctx.fillRect(0, 0, w, h);

      if (!xVals || xVals.length === 0) {
        return;
      }

      // Determine ranges
      let xmin = Math.min.apply(null, xVals);
      let xmax = Math.max.apply(null, xVals);
      if (xmin === xmax) xmax = xmin + 1;

      let ymin = Infinity;
      let ymax = -Infinity;
      series.forEach((s) => {
        s.forEach((v) => {
          if (v < ymin) ymin = v;
          if (v > ymax) ymax = v;
        });
      });
      if (ymin === Infinity || ymax === -Infinity || ymin === ymax) {
        ymin = ymin === Infinity ? -1 : ymin - 1;
        ymax = ymax === -Infinity ? 1 : ymax + 1;
      }

      // Draw grid lines
      const grid = this.opts.axes || [];
      ctx.strokeStyle = '#222';
      ctx.lineWidth = 1;
      ctx.beginPath();
      const verticalLines = 8;
      for (let i = 0; i <= verticalLines; i++) {
        const x = (i / verticalLines) * w;
        ctx.moveTo(x, 0);
        ctx.lineTo(x, h);
      }
      const horizontalLines = 8;
      for (let j = 0; j <= horizontalLines; j++) {
        const y = (j / horizontalLines) * h;
        ctx.moveTo(0, y);
        ctx.lineTo(w, y);
      }
      ctx.stroke();

      // Draw series
      series.forEach((s, idx) => {
        const stroke = (this.opts.series && this.opts.series[idx + 1]?.stroke) || '#0f0';
        ctx.strokeStyle = stroke;
        ctx.lineWidth = (this.opts.series && this.opts.series[idx + 1]?.width) || 1.5;
        ctx.beginPath();
        s.forEach((yVal, i) => {
          const x = w * ((xVals[i] - xmin) / (xmax - xmin));
          const y = h - h * ((yVal - ymin) / (ymax - ymin));
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.stroke();
      });
    }
  }

  global.uPlot = UPlotLite;
})(typeof window !== 'undefined' ? window : globalThis);
