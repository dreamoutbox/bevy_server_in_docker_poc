use std::{
    thread,
    time::{Duration, Instant},
};

use bevy::prelude::*;

use super::*;

#[derive(Resource, Default)]
struct CapturedOutput(Option<AsyncWorkOutput>);

fn poll_test_task(
    mut commands: Commands,
    mut query: Query<(Entity, &mut AsyncTask<AsyncWorkOutput>)>,
    mut captured: ResMut<CapturedOutput>,
) {
    for (entity, mut task) in &mut query {
        if let Some(output) = task.try_take() {
            captured.0 = Some(output);
            commands.entity(entity).despawn();
        }
    }
}

#[test]
fn async_task_completes_on_bevy_task_pool() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .init_resource::<CapturedOutput>()
        .add_systems(Update, poll_test_task);

    app.world_mut().spawn(AsyncTask::spawn(async {
        simulate_async_work("unit-job", Duration::from_millis(15)).await
    }));

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        app.update();
        if app.world().resource::<CapturedOutput>().0.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }

    let output = app
        .world()
        .resource::<CapturedOutput>()
        .0
        .clone()
        .expect("async task must finish before deadline");
    assert_eq!(output.summary, "unit-job completed");
    assert!(output.elapsed_ms >= 10);
}
