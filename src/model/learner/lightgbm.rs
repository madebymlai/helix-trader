//! LightGBM: gradient-boosted trees with the binary objective.

use std::ffi::CString;

use anyhow::{Context, ensure};
use lightgbm3::Booster;
use serde::{Deserialize, Serialize};

use super::{Learner, Predictor, SavedModel, TrainingSet};

/// LightGBM's settings, each named and meant as in LightGBM's parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct LightGbmConfig {
    /// The number of boosting rounds, one tree each.
    #[serde(default = "default_num_iterations")]
    #[builder(default = default_num_iterations())]
    pub num_iterations: u32,
    #[serde(default = "default_learning_rate")]
    #[builder(default = default_learning_rate())]
    pub learning_rate: f64,
    /// The most leaves of a tree.
    #[serde(default = "default_num_leaves")]
    #[builder(default = default_num_leaves())]
    pub num_leaves: u32,
    /// The fewest rows in a leaf.
    #[serde(default = "default_min_data_in_leaf")]
    #[builder(default = default_min_data_in_leaf())]
    pub min_data_in_leaf: u32,
    /// The share of the features drawn for each tree.
    #[serde(default = "default_feature_fraction")]
    #[builder(default = default_feature_fraction())]
    pub feature_fraction: f64,
    /// The seed of every draw.
    #[serde(default = "default_seed")]
    #[builder(default = default_seed())]
    pub seed: u32,
}

const fn default_num_iterations() -> u32 {
    100
}

const fn default_learning_rate() -> f64 {
    0.1
}

const fn default_num_leaves() -> u32 {
    31
}

const fn default_min_data_in_leaf() -> u32 {
    20
}

const fn default_feature_fraction() -> f64 {
    1.0
}

const fn default_seed() -> u32 {
    1
}

impl LightGbmConfig {
    /// The settings as LightGBM's parameters, `name=value` apart by
    /// spaces. LightGBM, built without OpenMP, trains on one thread;
    /// `deterministic` and `force_row_wise` keep its results from depending
    /// on the machine.
    fn parameters(&self) -> CString {
        let parameters = format!(
            "objective=binary num_iterations={} learning_rate={} num_leaves={} \
             min_data_in_leaf={} feature_fraction={} seed={} deterministic=true \
             force_row_wise=true num_threads=1 verbosity=-1",
            self.num_iterations,
            self.learning_rate,
            self.num_leaves,
            self.min_data_in_leaf,
            self.feature_fraction,
            self.seed,
        );
        CString::new(parameters).expect("numbers and names hold no NUL")
    }
}

impl Learner for LightGbmConfig {
    fn fit(&self, set: &TrainingSet) -> anyhow::Result<Box<dyn Predictor>> {
        let features: Vec<f64> = set.rows().flatten().copied().collect();
        let labels: Vec<f32> = set
            .labels()
            .iter()
            .map(|&label| f32::from(u8::from(label)))
            .collect();
        let text = c_api::train(
            &features,
            &labels,
            i32::try_from(set.width())?,
            &self.parameters(),
            self.num_iterations,
        )?;
        Ok(Box::new(LightGbm::new(Booster::from_string(&text)?)?))
    }

    fn load(&self, saved: &SavedModel) -> anyhow::Result<Box<dyn Predictor>> {
        let booster = Booster::from_string(saved.as_str()).context("not a saved LightGBM model")?;
        Ok(Box::new(LightGbm::new(booster)?))
    }
}

/// A LightGBM booster and the width of its rows.
struct LightGbm {
    booster: Booster,
    width: i32,
}

impl LightGbm {
    fn new(booster: Booster) -> anyhow::Result<Self> {
        let width = booster.num_features();
        ensure!(width > 0, "a LightGBM model without features");
        Ok(Self { booster, width })
    }
}

impl Predictor for LightGbm {
    fn width(&self) -> usize {
        self.width.unsigned_abs() as usize
    }

    fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        Ok(self.booster.predict(row, self.width, true)?[0])
    }

    fn save(&self) -> anyhow::Result<SavedModel> {
        Ok(SavedModel::from(self.booster.save_string()?))
    }
}

/// Training through LightGBM's C API. The binding builds its `Dataset`
/// without parameters, so the ones LightGBM reads when it builds a dataset
/// would keep LightGBM's defaults: the fewest rows in a leaf among them,
/// for which it drops every feature no split leaves that many rows each
/// side of, and on a small set would keep none and fit the training mean.
/// Here the dataset and the booster are built with the same parameters,
/// and the trained model is handed to the binding as its text.
mod c_api {
    use std::{
        ffi::{CStr, c_char, c_int},
        ptr,
    };

    use anyhow::bail;
    use lightgbm3_sys as sys;

    /// The text of a model of `iterations` rounds, at most, trained on
    /// `features`, rows of `width` one after the other, and their `labels`,
    /// with `parameters` building both the dataset and the booster.
    pub fn train(
        features: &[f64],
        labels: &[f32],
        width: i32,
        parameters: &CStr,
        iterations: u32,
    ) -> anyhow::Result<String> {
        let rows = i32::try_from(labels.len())?;
        let mut dataset = Dataset(ptr::null_mut());
        // SAFETY: `features` holds `rows` rows of `width` row-major f64s,
        // `labels` `rows` f32s, LightGBM copies both before returning.
        check(unsafe {
            sys::LGBM_DatasetCreateFromMat(
                features.as_ptr().cast(),
                sys::C_API_DTYPE_FLOAT64 as c_int,
                rows,
                width,
                1,
                parameters.as_ptr(),
                ptr::null_mut(),
                &raw mut dataset.0,
            )
        })?;
        check(unsafe {
            sys::LGBM_DatasetSetField(
                dataset.0,
                c"label".as_ptr(),
                labels.as_ptr().cast(),
                rows,
                sys::C_API_DTYPE_FLOAT32 as c_int,
            )
        })?;
        let mut booster = Booster(ptr::null_mut());
        // SAFETY: the dataset outlives the booster, dropped before it.
        check(unsafe {
            sys::LGBM_BoosterCreate(dataset.0, parameters.as_ptr(), &raw mut booster.0)
        })?;
        for _ in 0..iterations {
            let mut finished: c_int = 0;
            check(unsafe { sys::LGBM_BoosterUpdateOneIter(booster.0, &raw mut finished) })?;
            if finished != 0 {
                break;
            }
        }
        booster.text()
    }

    struct Dataset(sys::DatasetHandle);

    impl Drop for Dataset {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: a handle LightGBM created, freed once.
                unsafe { sys::LGBM_DatasetFree(self.0) };
            }
        }
    }

    struct Booster(sys::BoosterHandle);

    impl Booster {
        /// The model as LightGBM saves it, every iteration.
        fn text(&self) -> anyhow::Result<String> {
            let save = |buffer: &mut [u8], length: &mut i64| {
                // SAFETY: LightGBM writes at most `buffer.len()` bytes.
                check(unsafe {
                    sys::LGBM_BoosterSaveModelToString(
                        self.0,
                        0,
                        -1,
                        0,
                        i64::try_from(buffer.len()).expect("a buffer within i64"),
                        length,
                        buffer.as_mut_ptr().cast::<c_char>(),
                    )
                })
            };
            let mut length = 0;
            save(&mut [], &mut length)?;
            let mut buffer = vec![0; usize::try_from(length)?];
            save(&mut buffer, &mut length)?;
            Ok(CStr::from_bytes_with_nul(&buffer)?.to_str()?.to_string())
        }
    }

    impl Drop for Booster {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: a handle LightGBM created, freed once.
                unsafe { sys::LGBM_BoosterFree(self.0) };
            }
        }
    }

    /// LightGBM's last error if `code` is not success.
    fn check(code: c_int) -> anyhow::Result<()> {
        match code {
            0 => Ok(()),
            // SAFETY: LightGBM's last error is a NUL-terminated string.
            _ => bail!(
                "{}",
                unsafe { CStr::from_ptr(sys::LGBM_GetLastError()) }.to_string_lossy()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::learner::LearnerConfig;

    /// 300 rows of two features whose label is true exactly where the
    /// first feature is missing; elsewhere it runs from 0 to 1, zero
    /// included.
    fn missing_is_true() -> TrainingSet {
        let mut features = Vec::new();
        let mut labels = Vec::new();
        for i in 0..300_u32 {
            let missing = i % 3 == 0;
            features.push(if missing {
                f64::NAN
            } else {
                f64::from(i % 7) / 6.0
            });
            features.push(f64::from(i % 11));
            labels.push(missing);
        }
        TrainingSet::new(2, features, labels).unwrap()
    }

    #[test]
    fn a_missing_feature_is_learned_apart_from_every_value() {
        let config = LearnerConfig::LightGbm(LightGbmConfig::builder().build());
        let model = config.fit(&missing_is_true()).unwrap();
        assert!(model.predict(&[f64::NAN, 4.0]).unwrap() > 0.9);
        for value in [0.0, 0.5, 1.0] {
            assert!(model.predict(&[value, 4.0]).unwrap() < 0.1, "{value}");
        }
    }

    /// 30 rows of one feature, 0 to 29, true from 15 on: a split at 15
    /// leaves 15 rows each side, fewer than LightGBM's default fewest rows
    /// in a leaf, 20.
    #[test]
    fn fewer_rows_in_a_leaf_than_the_default_split_a_small_set() {
        let config = LightGbmConfig::builder().min_data_in_leaf(5).build();
        let set = TrainingSet::new(
            1,
            (0..30_u32).map(f64::from).collect(),
            (0..30_u32).map(|i| i >= 15).collect(),
        )
        .unwrap();
        let model = LearnerConfig::LightGbm(config).fit(&set).unwrap();
        let low = model.predict(&[2.0]).unwrap();
        let high = model.predict(&[27.0]).unwrap();
        assert!(low < 0.5 && high > 0.5, "{low} {high}");
    }

    #[test]
    fn the_seed_reaches_lightgbm() {
        let fit = |seed| {
            let config = LightGbmConfig::builder()
                .feature_fraction(0.5)
                .seed(seed)
                .build();
            let set = TrainingSet::new(
                3,
                (0..600_u32)
                    .map(|i| f64::from((i * 7919) % 101) / 100.0)
                    .collect(),
                (0..200_u32).map(|i| (i * 31) % 7 < 3).collect(),
            )
            .unwrap();
            let model = LearnerConfig::LightGbm(config).fit(&set).unwrap();
            model.predict(&[0.2, 0.5, 0.7]).unwrap().to_bits()
        };
        assert_ne!(fit(1), fit(2));
    }
}
