angular.module('barGauge', []).directive('barGauge', function () {

  return {
    restrict: 'A',
    scope: {
      baseInfo: '=',
    },
    isolate: false,
    templateUrl: 'components/bar_gauge/bar_gauge.html',
    controller: function ($scope) {
      $scope.$watch('baseInfo', function (options) {
        if (!options) {
          return;
        }
        var sum = 0;
        var newOptions = _.cloneDeep(options);
        _.each(newOptions.items, function (item, i) {
          if (item.renderedValue) {
            return false;
          } else {
            item.renderedValue = _.formatMemSize(item.value);
          }
        });
        var items = newOptions.items;
        var values = _.map(items, function (item) {
          return Math.max(item.value, 0);
        });
        var total = _(values).reduce(function (sum, num) {
          return sum + num;
        });

        values = _.rescaleForSum(100, values, total);

        _.each(values, function (item, i) {
          var v = values[i];
          values[i] += sum;
          newOptions.items[i].itemStyle.width = values[i] + "%";
          sum += v;
        });
        _.each(newOptions.markers, function (marker) {
          var percent = _.calculatePercent(marker.value, total);
          var i;
          if (_.indexOf(values, percent) < 0 && (i = _.indexOf(values, percent+1)) >= 0) {
            // if we're very close to some value, stick to it, so that
            // rounding error is not visible
            if (items[i].value - marker.value < sum*0.01) {
              percent++;
            }
          }
          marker.itemStyle = marker.itemStyle || {};
          marker.itemStyle.left = (percent > 100 ? 100 : percent) + '%';
        });
        newOptions.tdItems = _.select(newOptions.items, function (item) {
          return item.name !== null;
        });

        $scope.config = newOptions;
      }, true);
    }
  };
});

function UsageGaugeWidget($scope) {
}
